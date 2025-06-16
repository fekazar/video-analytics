package edu.misis.runner

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Instant
import java.util.UUID

const val CHUNKS_FOR_INFERENCE_TOPIC = "chunks-for-inference"

data class ChunkMessageData(
    val streamUrl: URI,
    val preSharedUrl: URI,
    val createdAt: Instant,
    val fps: Int,
)

@Component
class Inference(
    private val kafkaTemplate: KafkaTemplate<String, ChunkMessageData>
) {
    fun startInference(streamId: UUID, message: ChunkMessageData) {
        kafkaTemplate.send(
            CHUNKS_FOR_INFERENCE_TOPIC,
            streamId.toString(),
            message,
        )
    }
}