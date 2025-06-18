package edu.misis.runner

import edu.misis.runner.repository.InferenceRepository
import edu.misis.runner.repository.InferenceResultEntity
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Instant
import java.util.UUID

const val CHUNKS_FOR_INFERENCE_TOPIC = "chunks-for-inference"
const val INFERENCE_RESULT_TOPIC = "inference-results"

data class ChunkMessageData(
    val streamUrl: URI,
    val preSharedUrl: URI,
    val createdAt: Instant,
    val fps: Int,
)

data class InferenceResultData(
    val timestamp: Instant,
    val facesCount: Int,
)

@Component
class Inference(
    private val kafkaTemplate: KafkaTemplate<String, ChunkMessageData>,
    private val inferenceRepository: InferenceRepository,
) {
    private val logger = LoggerFactory.getLogger(Inference::class.java)

    fun startInference(streamId: UUID, message: ChunkMessageData) = kafkaTemplate.send(
        CHUNKS_FOR_INFERENCE_TOPIC,
        streamId.toString(),
        message,
    )

    @KafkaListener(
        groupId = "persistTimeSeriesGroup",
        topics = [INFERENCE_RESULT_TOPIC],
        batch = "true",
        concurrency = "1",
        containerFactory = "inferenceResultsContainerFactory",
    )
    fun processResults(records: ConsumerRecords<String, InferenceResultData>) {
        logger.info("Fetched: ${records.count()} records")
        records.map { InferenceResultEntity(it.value().timestamp, UUID.fromString(it.key()), it.value().facesCount) }
            .also { inferenceRepository.batchInsert(it) }
        logger.info("Saved fetched results")
    }
}