package edu.misis.runner

import io.minio.*
import io.minio.http.Method
import org.quartz.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.quartz.QuartzJobBean
import org.springframework.transaction.support.TransactionTemplate
import java.io.*
import java.net.URI
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.pow


const val WIDTH = 1280
const val HEIGHT = 720
const val FPS = 10
const val CHUNK_DURATION_SECONDS = 10

private class Chunk(
    val createdAt: Instant,
    val data: ByteArray,
)

private class ChunkReader(
    inputStream: InputStream,
) : AutoCloseable {
    private val bytesPerFrame = WIDTH * HEIGHT * 3 // RGB = 3 bytes/pixel
    private val bufferSize = bytesPerFrame * FPS * CHUNK_DURATION_SECONDS
    private val bufferedStream = BufferedInputStream(inputStream, bufferSize)

    private val logger = LoggerFactory.getLogger(ChunkReader::class.java)

    fun readChunks(): Sequence<Chunk> = sequence {
        var hasReadWholeChunk = true
        while (hasReadWholeChunk) {
            val buffer = ByteArray(bufferSize)
            val createdAt = Instant.now()
            val bytesRead = try {
                bufferedStream.readNBytes(buffer, 0, bufferSize)
            } catch (e: IOException) {
                logger.warn("Exception awaiting new chunk ${e.message}")
                0
            }

            logger.info("Raw chunk bytes read: $bytesRead")
            if (bytesRead != bufferSize) {
                hasReadWholeChunk = false
            } else {
                yield(Chunk(createdAt, buffer))
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
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()

        logger.info("Encoder subprocess: ${process.pid()}")

        val chunksSupplyingFuture = CompletableFuture.runAsync {
            process.outputStream.write(chunk)
            process.outputStream.flush()
            process.outputStream.close()
        }

        val result = process.inputStream.readAllBytes()

        val code = process.waitFor()
        chunksSupplyingFuture.cancel(true)
        if (code != 0) {
            throw IllegalStateException("Encoder subprocess failed")
        }
        return result
    }
}

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
class StreamChunkingJob : QuartzJobBean() {
    companion object {
        const val JOB_GROUP = "streamChunking"
        const val STREAM_ID_KEY = "id"
        const val RETRY_COUNT = "retryCount"
        const val RETRY_LIMIT = 3
    }

    @Autowired
    private lateinit var streamRepository: StreamRepository

    @Autowired
    private lateinit var s3Client: MinioClient

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, StreamEventData>

    @Autowired
    private lateinit var transactionTemplate: TransactionTemplate

    @Autowired
    private lateinit var scheduler: Scheduler

    @Autowired
    private lateinit var inference: Inference

    private val logger = LoggerFactory.getLogger(StreamChunkingJob::class.java)
    private val encoder = ChunkMP4Encoder()
    private val scheduledExecutor = Executors.newScheduledThreadPool(1)

    override fun executeInternal(context: JobExecutionContext) {
        logger.info("${StreamChunkingJob::class.simpleName} started")

        val streamId = UUID.fromString(context.mergedJobDataMap.getString(STREAM_ID_KEY)) ?:
            throw IllegalStateException("Stream id is missing")

        // Do not start job for non-existent stream
        // Start stream only for active record on job recovery
        streamRepository.findById(streamId)?.let { currentStream ->
            if (currentStream.state == StreamState.IN_PROGRESS) {
                startStream(currentStream, context)
            } else {
                logger.info("Stream was in inappropriate state (${currentStream.state} on job start. Terminating.")
                kafkaTemplate.send(
                    STATE_MACHINE_EVENTS_TOPIC,
                    currentStream.id.toString(),
                    StreamEventData(StreamEvent.STREAM_TERMINATED, emptyMap()),
                )
            }
        }
    }

    private fun startStream(stream: StreamEntity, context: JobExecutionContext) {
        val videoCaptureSubprocess = ProcessBuilder(
            "ffmpeg",
            "-rtsp_transport", "tcp",
            "-i", stream.streamUrl,
            "-vf", "scale=$WIDTH:$HEIGHT,fps=$FPS",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-c:v", "rawvideo",
            "-loglevel", "fatal",
            "-hide_banner",
            "-nostats",
            "-"
        ).redirectError(ProcessBuilder.Redirect.PIPE)
            .start()

        logger.info("Started video capture subprocess: ${videoCaptureSubprocess!!.pid()}")

        val completedByRequest = AtomicBoolean(false)

        val terminationMonitoringFuture = scheduledExecutor.scheduleAtFixedRate({
            val actualStream = streamRepository.findById(stream.id)
            if (actualStream?.state == StreamState.AWAIT_TERMINATION) {
                videoCaptureSubprocess.destroy()
                logger.info("ffmpeg stream was stopped")
                completedByRequest.set(true)
            }
        },
            0,
            2,
            TimeUnit.SECONDS
        )

        val fatalErrorsFuture = CompletableFuture.supplyAsync {
            val reader = BufferedReader(InputStreamReader(videoCaptureSubprocess.errorStream))
            reader.use { it.readLines() }
        }

        val encodingExecutor = Executors.newSingleThreadExecutor()

        ChunkReader(videoCaptureSubprocess.inputStream).use { reader ->
            reader.readChunks()
                .forEach { chunk ->
                    encodingExecutor.execute {
                        val encodedChunk = encoder.encode(chunk.data)
                        logger.info("Encoded chunk size: ${encodedChunk.size}")

                        val objectWriteResponse = uploadChunk(stream, encodedChunk)
                        val url = getPreSharedUrl(objectWriteResponse.bucket(), objectWriteResponse.`object`())

                        inference.startInference(
                            stream.id,
                            ChunkMessageData(
                                URI(stream.streamUrl),
                                url,
                                chunk.createdAt,
                                FPS,
                            )
                        )
                    }
                }
        }

        // Stop polling db
        runCatching {
            terminationMonitoringFuture.cancel(true)
            terminationMonitoringFuture.get()
        }

        val code = videoCaptureSubprocess.waitFor()
        logger.info("Video capturing process finished with code: $code")

        val fatalErrors = fatalErrorsFuture.get()
        if (fatalErrors.isNotEmpty()) {
            logger.error("Encountered following ffmpeg fatal errors:")
            fatalErrors.forEach {
                logger.error("ffmpeg error for stream ${stream.id}: $it")
            }
        }

        if (!completedByRequest.get()) {
            if (fatalErrors.isNotEmpty()) {
                // do not restart job after limit was exceeded
                val currentRetryCount = if (context.mergedJobDataMap.containsKey(RETRY_COUNT)) {
                    context.mergedJobDataMap.getIntValue(RETRY_COUNT)
                } else {
                    0
                }
                if (currentRetryCount != RETRY_LIMIT - 1) {
                    logger.warn(
                        "Rescheduling job for for stream: ${stream.id}, ${stream.streamUrl}. Attempt ${currentRetryCount + 1} / $RETRY_LIMIT"
                    )

                    val chunkingJob = JobBuilder.newJob(StreamChunkingJob::class.java)
                        .withIdentity(stream.streamUrl, JOB_GROUP)
                        .usingJobData(STREAM_ID_KEY, stream.id.toString())
                        .usingJobData(RETRY_COUNT, (currentRetryCount + 1).toString())
                        .requestRecovery()
                        .build()

                    val delayMillis = ((2.0 + Math.random()).pow(currentRetryCount + 1) * 1000).toLong()

                    val trigger = TriggerBuilder.newTrigger()
                        .startAt(Date.from(Instant.now().plusMillis(delayMillis)))
                        .forJob(chunkingJob)
                        .build()

                    transactionTemplate.executeWithoutResult {
                        scheduler.deleteJob(chunkingJob.key)
                        scheduler.scheduleJob(chunkingJob, trigger)
                    }
                } else {
                    logger.warn("Stream chunking job retry limit exceeded for stream: ${stream.id}, ${stream.streamUrl}. Terminating.")
                    kafkaTemplate.send(
                        STATE_MACHINE_EVENTS_TOPIC,
                        stream.id.toString(),
                        StreamEventData(StreamEvent.STREAM_TERMINATED, emptyMap()),
                    )
                }
            } else {
                // The process was killed due to instance restart
                throw JobExecutionException("StreamChunkingJob has completed unexpectedly, but without ffmpeg errors", true)
            }
        } else {
            scheduledExecutor.shutdown()
            try {
                scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)
            } catch (e: InterruptedException) {
                logger.warn("Couldn't await chunking job scheduler termination. Using force shutdown")
                scheduledExecutor.shutdownNow()
            }
            logger.info("Stream chunking job completes normally")

            kafkaTemplate.send(
                STATE_MACHINE_EVENTS_TOPIC,
                stream.id.toString(),
                StreamEventData(StreamEvent.STREAM_TERMINATED, emptyMap()),
            )
        }
    }

    private fun uploadChunk(stream: StreamEntity, encodedChunk: ByteArray): ObjectWriteResponse {
        val bytesStream = ByteArrayInputStream(encodedChunk)
        logger.info("Uploading to s3 for stream ${stream.id}...")
        val res = s3Client.putObject(
            PutObjectArgs.builder()
                .stream(bytesStream, encodedChunk.size.toLong(), ObjectWriteArgs.MIN_MULTIPART_SIZE.toLong())
                .bucket(stream.chunksBucket)
                .`object`(System.currentTimeMillis().toString())
                .contentType("video/mp4")
                .build()
        )
        logger.info("Chunk was uploaded for stream ${stream.id}")
        return res
    }

    private fun getPreSharedUrl(bucket: String, obj: String): URI {
        logger.info("Generating pre signed url...")
        val reqParams = mapOf("response-content-type" to "video/mp4")
        return s3Client.getPresignedObjectUrl(
            GetPresignedObjectUrlArgs.builder()
                .method(Method.GET)
                .bucket(bucket)
                .`object`(obj)
                .expiry(2, TimeUnit.HOURS)
                .extraQueryParams(reqParams)
                .build()
        ).let {
            logger.info("Generated presigned url")
            URI(it)
        }
    }
}