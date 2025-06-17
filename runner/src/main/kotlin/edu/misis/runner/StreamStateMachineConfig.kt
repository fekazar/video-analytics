package edu.misis.runner

import io.minio.MakeBucketArgs
import io.minio.MinioClient
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
    BUCKET_INITIALIZED,
    IN_PROGRESS,
    AWAIT_TERMINATION,
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

    fun initializeBucket(stream: StreamEntity) {
        val location = "chunks-${stream.id}"
        s3Client.makeBucket(MakeBucketArgs.builder().bucket(location).build())
        val updated = stream.copy(
            chunksBucket = location,
            state = StreamState.BUCKET_INITIALIZED,
        )
        streamRepository.update(updated)
        kafkaTemplate.send(STATE_MACHINE_EVENTS_TOPIC, stream.id.toString(), StreamEventData(StreamEvent.START_STREAM, emptyMap()))
    }

    fun startStream(stream: StreamEntity) {
        logger.info("Scheduling stream chunking job...")

        val sampleJob = JobBuilder.newJob(StreamChunkingJob::class.java)
            .withIdentity(stream.streamUrl.toString(), StreamChunkingJob.JOB_GROUP)
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
        streamRepository.updateState(stream.id, StreamState.AWAIT_TERMINATION)
    }

    fun clearStream(stream: StreamEntity) {
        streamRepository.deleteById(stream.id)
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
            when (stream.state) {
                StreamState.INITIAL -> if (event.type == StreamEvent.INITIALIZE_BUCKET) {
                    initializeBucket(stream)
                } else if (event.type == StreamEvent.STOP_STREAM) {
                    clearStream(stream)
                }

                StreamState.BUCKET_INITIALIZED -> if (event.type == StreamEvent.START_STREAM) {
                    startStream(stream)
                } else if (event.type == StreamEvent.STOP_STREAM || event.type == StreamEvent.STREAM_TERMINATED) {
                    // todo: consider deleting bucket
                    // No active stream to stop, clearing
                    clearStream(stream)
                }

                StreamState.IN_PROGRESS -> if (event.type == StreamEvent.STOP_STREAM) {
                    stopStream(stream)
                } else if (event.type == StreamEvent.STREAM_TERMINATED) {
                    clearStream(stream)
                }

                StreamState.AWAIT_TERMINATION -> if (event.type == StreamEvent.STREAM_TERMINATED) {
                    clearStream(stream)
                }
            }
        }.onFailure {
            logger.error("Failed to process event: ", it)
        }
    }
}