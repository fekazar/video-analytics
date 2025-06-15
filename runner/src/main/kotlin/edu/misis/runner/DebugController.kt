package edu.misis.runner

import jakarta.annotation.PostConstruct
import org.quartz.JobBuilder
import org.quartz.Scheduler
import org.quartz.TriggerBuilder
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.net.URI

@RestController
class DebugController(
    val scheduler: Scheduler,
    val streamRepository: StreamRepository,
    val transactionTemplate: TransactionTemplate,
) {
    private val logger = LoggerFactory.getLogger(DebugController::class.java)

    @PostConstruct
    fun postConstruct() {
        logger.info("Starting scheduler...")
        scheduler.start()
    }

    @PostMapping("/start-job")
    fun startJob(@RequestParam("streamUrl") streamUrl: String) {
        streamRepository.deleteIfTerminated(streamUrl)

        transactionTemplate.executeWithoutResult {
            val stream = streamRepository.createNewStream(streamUrl)
            if (stream != null) {
                val sampleJob = JobBuilder.newJob(StreamChunkingJob::class.java)
                    .withIdentity(streamUrl, StreamChunkingJob.JOB_GROUP)
                    .usingJobData(StreamChunkingJob.STREAM_ID_KEY, stream.id.toString())
                    .requestRecovery()
                    .build()

                val trigger = TriggerBuilder.newTrigger()
                    .startNow()
                    .build()

                scheduler.scheduleJob(sampleJob, trigger)
            }
        }
    }

    @PostMapping("/stop-job")
    fun stopJob(@RequestParam("streamUrl") streamUrl: URI) {
        streamRepository.initTermination(streamUrl.toString())
    }
}