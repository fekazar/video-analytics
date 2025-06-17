package edu.misis.runner

import org.quartz.Scheduler
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.util.*

data class StreamUrlAccepted(
    val streamId: UUID,
    val streamUrl: URI
)

@RestController
class StreamController(
    val streamRepository: StreamRepository,
    private val kafkaTemplate: KafkaTemplate<String, StreamEventData>,
) {
    private val logger = LoggerFactory.getLogger(StreamController::class.java)

    @PostMapping("/start-job")
    fun startJob(@RequestParam("streamUrl") streamUrl: URI): ResponseEntity<*> {
        logger.info("Start request for: $streamUrl")
        val stream = streamRepository.createNewStream(streamUrl)?.also {
            kafkaTemplate.send(
                STATE_MACHINE_EVENTS_TOPIC,
                it.id.toString(),
                StreamEventData(StreamEvent.INITIALIZE_BUCKET, emptyMap())
            )
        } ?: streamRepository.findByStreamUrl(streamUrl)

        return if (stream == null) {
            ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Unit)
        } else {
            ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(StreamUrlAccepted(stream.id, stream.streamUrl))
        }
    }

    @PostMapping("/stop-job")
    fun stopJob(@RequestParam("streamId") streamId: UUID): ResponseEntity<Unit> {
        kafkaTemplate.send(STATE_MACHINE_EVENTS_TOPIC, streamId.toString(), StreamEventData(StreamEvent.STOP_STREAM, emptyMap()))
        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .body(Unit)
    }
}