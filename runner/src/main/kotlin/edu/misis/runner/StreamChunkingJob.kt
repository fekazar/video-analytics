package edu.misis.runner

import io.minio.BucketArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.ObjectWriteArgs
import io.minio.PutObjectArgs
import io.minio.UploadObjectArgs
import org.quartz.DisallowConcurrentExecution
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.quartz.QuartzJobBean
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.util.*
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
    private lateinit var s3Client: MinioClient

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, StreamEventData>

    private val logger = LoggerFactory.getLogger(StreamChunkingJob::class.java)
    private val encoder = ChunkMP4Encoder()
    private val scheduledExecutor = Executors.newScheduledThreadPool(1)

    override fun executeInternal(context: JobExecutionContext) {
        logger.info("${StreamChunkingJob::class.simpleName} started")

        val streamId = UUID.fromString(context.mergedJobDataMap.getString(STREAM_ID_KEY)) ?:
            throw IllegalStateException("Stream id is missing")

        // Do not start stream for non-existent job
        // Start stream only for active record on job recovery
        val currentStream = streamRepository.findById(streamId)
        if (currentStream != null && currentStream.state == StreamState.IN_PROGRESS) {
            startStream(currentStream)
        }
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
            if (stream?.state == StreamState.AWAIT_TERMINATION) {
                videoCaptureSubprocess.destroy()
                logger.info("Stream was stopped, updating state")
                kafkaTemplate.send(
                    STATE_MACHINE_EVENTS_TOPIC, stream.id.toString(),
                    StreamEventData(StreamEvent.STREAM_TERMINATED, emptyMap()),
                )
            }
        },
            0,
            2,
            TimeUnit.SECONDS
        )

        val encodingExecutor = Executors.newSingleThreadExecutor()
        ChunkReader(videoCaptureSubprocess.inputStream).use { reader ->
            reader.readChunks()
                .forEach { chunk ->
                    encodingExecutor.execute {
                        val encodedChunk = encoder.encode(chunk)
                        logger.info("Encoded chunk size: ${encodedChunk.size}")

                        val stream = ByteArrayInputStream(encodedChunk)
                        logger.info("Uploading to s3 for stream ${streamEntity.id}...")
                        s3Client.putObject(
                            PutObjectArgs.builder()
                                .stream(stream, encodedChunk.size.toLong(), ObjectWriteArgs.MIN_MULTIPART_SIZE.toLong())
                                .bucket(streamEntity.chunksBucket)
                                .`object`(System.currentTimeMillis().toString())
                                .contentType("video/mp4")
                                .build()
                        )
                        logger.info("Chunk was uploaded for stream ${streamEntity.id}")
                    }
                }
        }

        // Cancel job to not schedule anymore
        runCatching {
            scheduleFuture.cancel(true)
            scheduleFuture.get()
        }
    }
}