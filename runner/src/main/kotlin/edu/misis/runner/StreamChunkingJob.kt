package edu.misis.runner

import org.quartz.DisallowConcurrentExecution
import org.quartz.InterruptableJob
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream

const val WIDTH = 640
const val HEIGHT = 480
const val FPS = 10
const val CHUNK_DURATION_SECONDS = 10

class ChunkReader(
    inputStream: InputStream,
) : AutoCloseable {
    private val bytesPerFrame = WIDTH * HEIGHT * 3 // RGB = 3 bytes/pixel
    private val bufferSize = bytesPerFrame * FPS * CHUNK_DURATION_SECONDS
    private val bufferedStream = BufferedInputStream(inputStream, bufferSize)

    private val logger = LoggerFactory.getLogger(ChunkReader::class.java)

    fun readChunks(): Sequence<ByteArray> = sequence {
        var hasReadWholeChunk = true
        val buffer = ByteArray(bufferSize)
        while (hasReadWholeChunk) {
            val bytesRead = bufferedStream.readNBytes(buffer, 0, bufferSize)
            logger.info("Bytes read: $bytesRead")
            if (bytesRead != bufferSize) {
                hasReadWholeChunk = false
            } else {
                yield(buffer.copyOf())
            }
        }
    }

    override fun close() {
        bufferedStream.close()
    }
}

class ChunkMP4Encoder {
    private val logger = LoggerFactory.getLogger(ChunkMP4Encoder::class.java)

    fun encode(chunk: ByteArray): ByteArray {
        logger.info("Starting chunk encoding")

        val process = ProcessBuilder(
            "ffmpeg",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-video_size", "${WIDTH}x$HEIGHT",
            "-framerate", "$FPS",
            "-i", "pipe:0",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-movflags", "frag_keyframe+empty_moov",
            "-f", "mp4",
            "pipe:1"
        ).redirectInput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.INHERIT) // todo: consider inherit
            .start()

        process.outputStream.write(chunk)
        process.outputStream.close()

        val result = process.inputStream.readAllBytes()

        val code = process.waitFor()
        if (code != 0) {
            throw IllegalStateException("Encoder subprocess failed")
        }
        return result
    }
}

@DisallowConcurrentExecution
class StreamChunkingJob : InterruptableJob {
    companion object {
        const val STREAM_URL = "streamUrl"
    }

    private val logger = LoggerFactory.getLogger(StreamChunkingJob::class.java)
    private val encoder = ChunkMP4Encoder()

    override fun execute(context: JobExecutionContext) {
        val streamUrl: String = context.jobDetail.jobDataMap.getString(STREAM_URL) ?:
            throw IllegalStateException("Stream url is missing")

        val process = ProcessBuilder(
            "ffmpeg",
            "-i", streamUrl,
            "-vf", "scale=$WIDTH:$HEIGHT,fps=$FPS",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-c:v", "rawvideo",
            "-"
        ).redirectError(ProcessBuilder.Redirect.INHERIT) // todo: consider inherit
            .start()

        logger.info("Started subprocess: ${process.pid()}")

        ChunkReader(process.inputStream).use { reader ->
            reader.readChunks()
                .forEachIndexed { idx, chunk ->
                    val encodedChunk = encoder.encode(chunk)
                    // todo: write encoded chunk to s3/redis
                    logger.info("Encoded chunk size: ${encodedChunk.size}")
                    //File("encoded-$idx.mp4").writeBytes(encodedChunk)
                }
        }

        process.waitFor()
    }

    override fun interrupt() {
        logger.info("Interruption callback was called")
        TODO("Not yet implemented")
    }
}