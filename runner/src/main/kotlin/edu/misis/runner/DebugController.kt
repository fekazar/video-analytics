package edu.misis.runner

import jakarta.annotation.PostConstruct
import org.quartz.Scheduler
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.util.*

@RestController
class DebugController(
    val scheduler: Scheduler,
    val streamRepository: StreamRepository,
    val transactionTemplate: TransactionTemplate,
    private val kafkaTemplate: KafkaTemplate<String, StreamEventData>,
) {
    private val logger = LoggerFactory.getLogger(DebugController::class.java)

    @PostConstruct
    fun postConstruct() {
        logger.info("Starting scheduler...")
        scheduler.start()
    }

    @PostMapping("/start-job")
    fun startJob(@RequestParam("streamUrl") streamUrl: String) {
        logger.info("Start request for: $streamUrl")

        streamRepository.deleteIfTerminated(streamUrl)
        transactionTemplate.executeWithoutResult {
            val stream = streamRepository.createNewStream(streamUrl)
            if (stream != null) {
                // todo: consider outbox
                kafkaTemplate.send(STATE_MACHINE_EVENTS_TOPIC, stream.id.toString(), StreamEventData(StreamEvent.INITIALIZE_BUCKET, emptyMap()))
            }
        }
    }

    @PostMapping("/stop-job")
    fun stopJob(@RequestParam("streamUrl") streamUrl: URI) {
        //kafkaTemplate.send(STATE_MACHINE_EVENTS_TOPIC, stream.id.toString(), StreamEventData(StreamEvent.INITIALIZE_BUCKET, emptyMap()))
    }
}