package edu.misis.runner

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import kotlinx.coroutines.runBlocking
import org.quartz.DisallowConcurrentExecution
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.quartz.QuartzJobBean
import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStream
import java.util.UUID
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
            val bytesRead = try {
                bufferedStream.readNBytes(buffer, 0, bufferSize)
            } catch (e: IOException) {
                logger.warn("Exception awaiting new chunk")
                0
            }

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
            .redirectError(ProcessBuilder.Redirect.DISCARD) // todo: consider inherit
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
class StreamChunkingJob : QuartzJobBean() {
    companion object {
        const val JOB_GROUP = "streamChunking"
        const val STREAM_URL_KEY = "streamUrl"
        const val STREAM_ID_KEY = "id"
    }

    @Autowired
    private lateinit var streamRepository: StreamRepository

    @Autowired
    private lateinit var s3Client: S3Client

    private val logger = LoggerFactory.getLogger(StreamChunkingJob::class.java)
    private val encoder = ChunkMP4Encoder()
    private val scheduledExecutor = Executors.newScheduledThreadPool(1)

    override fun executeInternal(context: JobExecutionContext) {
        val streamUrl: String = context.mergedJobDataMap.getString(STREAM_URL_KEY) ?:
            throw IllegalStateException("Stream url is missing")

        val streamId = UUID.fromString(context.mergedJobDataMap.getString(STREAM_ID_KEY)) ?:
            throw IllegalStateException("Stream id is missing")

        // Do not start stream for non-existent job
        // Do not start stream on job recovery
        val currentStream = streamRepository.findById(streamId)
        when (currentStream?.state) {
            DBStreamState.INIT_BUCKET -> TODO()
            DBStreamState.IN_PROGRESS -> startStream(currentStream)
            DBStreamState.AWAIT_TERMINATION, DBStreamState.TERMINATED, null -> {
                logger.warn("Stream $streamUrl was rejected due to incorrect state")
                // Since stream was not started, explicitly set state to terminated
                streamRepository.updateState(streamId, DBStreamState.TERMINATED)
                return
            }
        }
    }

    private fun initBucket(streamEntity: StreamEntity): StreamEntity = runBlocking {
        val createBucketResponse = s3Client.createBucket(CreateBucketRequest {
            bucket = "/chunks/${streamEntity.id}"
        })
        val updated = streamEntity.copy(
            state = DBStreamState.IN_PROGRESS,
            chunksBucket = createBucketResponse.location,
        )
        streamRepository.update(updated)
        updated
    }

    private fun startStream(streamEntity: StreamEntity) { // todo: add bucket-related arg
        val videoCaptureSubprocess = ProcessBuilder(
            "ffmpeg",
            "-rtsp_transport", "tcp",
            "-i", streamEntity.streamUrl,
            "-vf", "scale=$WIDTH:$HEIGHT,fps=$FPS",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-c:v", "rawvideo",
            "-"
        ).redirectError(ProcessBuilder.Redirect.DISCARD) // todo: consider inherit
            .start()
        logger.info("Started video capture subprocess: ${videoCaptureSubprocess!!.pid()}")

        val scheduleFuture = scheduledExecutor.scheduleAtFixedRate({
            val stream = streamRepository.findById(streamEntity.id)
            if (stream?.state == DBStreamState.AWAIT_TERMINATION) {
                videoCaptureSubprocess.destroy()
                logger.info("Stream was stopped, updating state")
                streamRepository.updateState(streamEntity.id, DBStreamState.TERMINATED)
            }
        },
            0,
            2,
            TimeUnit.SECONDS
        )

        val encodingExecutor = Executors.newSingleThreadExecutor()
        ChunkReader(videoCaptureSubprocess.inputStream).use { reader ->
            reader.readChunks()
                .forEachIndexed { idx, chunk ->
                    encodingExecutor.execute {
                        val encodedChunk = encoder.encode(chunk)
                        logger.info("Encoded chunk size: ${encodedChunk.size}")

                        // todo: write encoded chunk to s3/redis

                    }
                }
        }

        // Cancel job to not schedule anymore
        scheduleFuture.cancel(true)

    }
}