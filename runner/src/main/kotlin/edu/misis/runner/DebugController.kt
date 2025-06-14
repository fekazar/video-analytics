package edu.misis.runner

import jakarta.annotation.PostConstruct
import org.quartz.JobBuilder
import org.quartz.Scheduler
import org.quartz.TriggerBuilder
import org.quartz.impl.StdSchedulerFactory
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
    @PostConstruct
    fun postConstruct() {
        scheduler.start()
    }

    @PostMapping("/start-job")
    fun startJob(@RequestParam("streamUrl") streamUrl: String) {
        streamRepository.deleteIfTerminated(streamUrl)

        val sampleJob = JobBuilder.newJob(StreamChunkingJob::class.java)
            .withIdentity(streamUrl, StreamChunkingJob.JOB_GROUP)
            .usingJobData(StreamChunkingJob.STREAM_URL_KEY, streamUrl)
            .build()

        val trigger = TriggerBuilder.newTrigger()
            .startNow()
            .build()

        transactionTemplate.executeWithoutResult {
            val stream = streamRepository.upsert(streamUrl)
            if (stream != null) {
                scheduler.scheduleJob(sampleJob, trigger)
            }
        }
    }

    @PostMapping("/stop-job")
    fun stopJob(@RequestParam("streamUrl") streamUrl: URI) {
        streamRepository.updateState(streamUrl.toString(), StreamState.AWAIT_TERMINATION)
    }
}