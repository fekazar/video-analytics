package edu.misis.runner

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import jakarta.annotation.PostConstruct
import org.quartz.JobBuilder
import org.quartz.Scheduler
import org.quartz.TriggerBuilder
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.transaction.support.TransactionTemplate
import java.util.*

const val STATE_MACHINE_EVENTS_TOPIC = "state-machine-events"
const val STREAM_STATE_MACHINE_GROUP = "stream-state-machine-group"

enum class StreamState {
    INITIAL,
    INITIALIZING_BUCKET,
    BUCKET_INITIALIZED,
    IN_PROGRESS,
    AWAIT_TERMINATION,
    TERMINATED,
}

enum class StreamEvent {
    INITIALIZE_BUCKET,
    START_STREAM,
    STOP_STREAM,
    STREAM_TERMINATED,
}

data class StreamEventData(
    val type: StreamEvent,
    val headers: Map<String, String>,
)

@Configuration
class StreamStateMachineConfig(
    private val scheduler: Scheduler,
    private val kafkaTemplate: KafkaTemplate<String, StreamEventData>,
    private val streamRepository: StreamRepository,
    private val transactionTemplate: TransactionTemplate,
    private val s3Client: MinioClient,
) {
    private val logger = LoggerFactory.getLogger(StreamStateMachineConfig::class.java)

    @PostConstruct
    fun postConstruct() {
        scheduler.start()
    }

    fun initializeBucket(stream: StreamEntity) {
        val location = "chunks-${stream.id}"
        s3Client.makeBucket(MakeBucketArgs.builder().bucket(location).build())
        val updated = stream.copy(
            chunksBucket = location,
        )
        streamRepository.update(updated)
        kafkaTemplate.send(STATE_MACHINE_EVENTS_TOPIC, stream.id.toString(), StreamEventData(StreamEvent.START_STREAM, emptyMap()))
    }

    fun startStream(stream: StreamEntity) {
        logger.info("Scheduling stream chunking job...")

        val sampleJob = JobBuilder.newJob(StreamChunkingJob::class.java)
            .withIdentity(stream.streamUrl, StreamChunkingJob.JOB_GROUP)
            .usingJobData(StreamChunkingJob.STREAM_ID_KEY, stream.id.toString())
            .requestRecovery()
            .build()

        val trigger = TriggerBuilder.newTrigger()
            .startNow()
            .forJob(sampleJob)
            .build()

        transactionTemplate.executeWithoutResult {
            streamRepository.updateState(stream.id, StreamState.IN_PROGRESS)
            scheduler.scheduleJob(sampleJob, trigger)
        }
    }

    fun stopStream(stream: StreamEntity) {
        streamRepository.initTermination(stream.streamUrl)
    }

    fun clearStream(streamId: UUID) {
        // todo: clear record from db
    }

    @KafkaListener(
        topics = [STATE_MACHINE_EVENTS_TOPIC],
        groupId = STREAM_STATE_MACHINE_GROUP,
    )
    fun eventListener(
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        @Payload event: StreamEventData,
    ) {
        // restore state machine
        logger.info("Received event: $key - $event")
        val streamId = UUID.fromString(key)
        val stream = streamRepository.findById(streamId)
        if (stream == null) {
            logger.error("Unknown stream: $key")
            return
        }

        runCatching {
            // Since only one consumer can modify the state of the stream, it is safe to perform checks as below
            when (event.type) {
                StreamEvent.INITIALIZE_BUCKET -> initializeBucket(stream)
                StreamEvent.START_STREAM -> startStream(stream)
                StreamEvent.STOP_STREAM -> if (stream.state in setOf(
                        StreamState.INITIAL,
                        StreamState.BUCKET_INITIALIZED,
                        StreamState.IN_PROGRESS
                    )
                ) {
                    stopStream(stream)
                }

                StreamEvent.STREAM_TERMINATED -> clearStream(streamId)
            }
        }.onFailure {
            logger.error("Failed to process event: ", it)
        }
    }
}