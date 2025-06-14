package edu.misis.runner

import org.quartz.DisallowConcurrentExecution
import org.quartz.InterruptableJob
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.quartz.QuartzJobBean
import java.io.BufferedInputStream
import java.io.InputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

const val WIDTH = 1280
const val HEIGHT = 720
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
            logger.info("Raw chunk bytes read: $bytesRead")
            if (bytesRead != bufferSize) {
                hasReadWholeChunk = false
            } else {
                // todo: try allocating new array instead of copy
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

        logger.info("Encoder subprocess: ${process.pid()}")

        CompletableFuture.runAsync {
            process.outputStream.write(chunk)
            process.outputStream.flush()
            process.outputStream.close()
        }

        val result = process.inputStream.readAllBytes()

        val code = process.waitFor()
        if (code != 0) {
            throw IllegalStateException("Encoder subprocess failed")
        }
        return result
    }
}

@DisallowConcurrentExecution
class StreamChunkingJob : InterruptableJob, QuartzJobBean() {
    companion object {
        const val JOB_GROUP = "streamChunking"
        const val STREAM_URL_KEY = "streamUrl"
    }

    @Autowired
    private lateinit var streamRepository: StreamRepository

    private val logger = LoggerFactory.getLogger(StreamChunkingJob::class.java)
    private val encoder = ChunkMP4Encoder()
    private val scheduledExecutor = Executors.newScheduledThreadPool(1)

    private var videoCaptureSubprocess: Process? = null

    override fun executeInternal(context: JobExecutionContext) {
        val streamUrl: String = context.mergedJobDataMap.getString(STREAM_URL_KEY) ?:
            throw IllegalStateException("Stream url is missing")

        // Do not start stream for non-existent job
        // Do not start stream on job recovery
        val currentStream = streamRepository.findById(streamUrl)
        if (currentStream == null || currentStream.state != StreamState.IN_PROGRESS) {
            logger.warn("Stream $streamUrl was rejected due to incorrect state")
            // Since stream was not started, explicitly set state to terminated
            streamRepository.updateState(streamUrl, StreamState.TERMINATED)
            return
        }

        videoCaptureSubprocess = ProcessBuilder(
            "ffmpeg",
            "-rtsp_transport", "tcp",
            "-i", streamUrl,
            "-vf", "scale=$WIDTH:$HEIGHT,fps=$FPS",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-c:v", "rawvideo",
            "-"
        ).redirectError(ProcessBuilder.Redirect.DISCARD) // todo: consider inherit
            .start()

        logger.info("Started video capture subprocess: ${videoCaptureSubprocess!!.pid()}")

        val encodingExecutor = Executors.newSingleThreadExecutor()

        ChunkReader(videoCaptureSubprocess!!.inputStream).use { reader ->
            reader.readChunks()
                .forEachIndexed { idx, chunk ->
                    encodingExecutor.execute {
                        val encodedChunk = encoder.encode(chunk)
                        // todo: write encoded chunk to s3/redis
                        logger.info("Encoded chunk size: ${encodedChunk.size}")
                        //File("encoded-$idx.mp4").writeBytes(encodedChunk)
                    }
                }
        }

        val scheduledFuture = scheduledExecutor.scheduleAtFixedRate({
            val stream = streamRepository.findById(streamUrl)
            if (stream?.state == StreamState.AWAIT_TERMINATION) {
                throw IllegalStateException("Stream should be terminated")
            }
        },
            0,
            2,
            TimeUnit.SECONDS
        )

        runCatching {
            scheduledFuture.get()
        }.onFailure {
            if (it.cause is IllegalStateException) {
                logger.info("Closing stream due to state change")
                videoCaptureSubprocess!!.destroy()
            } else {
                logger.error("Unexpected exception during status check:", it.cause)
            }
        }

    }

    override fun interrupt() {
        logger.info("Interrupting rtsp client subprocess...")
        if (videoCaptureSubprocess != null) {
            videoCaptureSubprocess!!.destroy()
            logger.info("Video capture subprocess was destroyed")
        } else {
            logger.info("Video capture subprocess was not initialized")
        }
    }
}