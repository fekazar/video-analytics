package edu.misis.runner

import org.quartz.JobBuilder
import org.quartz.TriggerBuilder
import org.quartz.TriggerKey
import org.quartz.impl.StdSchedulerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class DebugController {
    private val scheduler = StdSchedulerFactory().scheduler

    init {
        scheduler.start()
    }

    @PostMapping("/start-job")
    fun startJob() {
        val sampleJob = JobBuilder.newJob(StreamChunkingJob::class.java)
            .usingJobData(StreamChunkingJob.STREAM_URL, "rtsp://localhost:8554/mystream")
            .build()

        val trigger = TriggerBuilder.newTrigger()
            .withIdentity(TriggerKey.triggerKey("test123"))
            .startNow()
            .build()

        scheduler.scheduleJob(sampleJob, trigger)
    }

    @PostMapping("/stop-job")
    fun stopJob() {
        scheduler.unscheduleJob(TriggerKey.triggerKey("test123"))
    }
}