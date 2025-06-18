package edu.misis.runner

import edu.misis.runner.repository.StreamRepository
import org.quartz.DisallowConcurrentExecution
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.quartz.QuartzJobBean
import java.time.Instant
import java.util.concurrent.CompletableFuture

@DisallowConcurrentExecution
class StalledStreamMonitoringJob : QuartzJobBean() {
    private val logger = LoggerFactory.getLogger(StalledStreamMonitoringJob::class.java)

    @Autowired
    private lateinit var streamRepository: StreamRepository

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, StreamEventData>

    override fun executeInternal(context: JobExecutionContext) {
        // Should be large enough to consider time for chunking job retires
        val threshold = Instant.now().minusSeconds(60)
        streamRepository.findStalledStreams(threshold)
            .also { logger.info("Found ${it.size} stalled streams") }
            .map { stalledStream ->
                CompletableFuture.runAsync {
                    when (stalledStream.state) {
                        StreamState.INITIAL -> StreamEventData(StreamEvent.INITIALIZE_BUCKET)
                        StreamState.BUCKET_INITIALIZED -> StreamEventData(StreamEvent.START_STREAM)
                        // Since chunking job is considered dead, push TERMINATED event
                        StreamState.IN_PROGRESS -> StreamEventData(StreamEvent.STREAM_TERMINATED)
                        StreamState.AWAIT_TERMINATION -> StreamEventData(StreamEvent.STREAM_TERMINATED)
                        StreamState.TERMINATED -> null
                    }?.let {
                        kafkaTemplate.send(STREAM_STATE_MACHINE_EVENTS_TOPIC, stalledStream.id.toString(), it)
                    }
                }
            }.forEach {
                it.get()
            }
    }
}
