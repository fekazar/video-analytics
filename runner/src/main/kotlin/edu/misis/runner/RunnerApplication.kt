package edu.misis.runner

import org.quartz.JobBuilder
import org.quartz.ObjectAlreadyExistsException
import org.quartz.ScheduleBuilder
import org.quartz.Scheduler
import org.quartz.SchedulerException
import org.quartz.SimpleScheduleBuilder
import org.quartz.TriggerBuilder
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.runApplication
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate

@SpringBootApplication
class RunnerApplication

@Component
class ApplicationReadyListener(
	private val scheduler: Scheduler,
	private val transactionTemplate: TransactionTemplate,
) {
	private val logger = LoggerFactory.getLogger(ApplicationReadyListener::class.java)

	@EventListener(value = [ApplicationReadyEvent::class])
	fun onApplicationReady() {
		val job = JobBuilder.newJob(StalledStreamMonitoringJob::class.java)
			.withIdentity(StalledStreamMonitoringJob::class.simpleName)
			.build()

		val trigger = TriggerBuilder.newTrigger()
			.forJob(job)
			.withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(5))
			.build()

		transactionTemplate.executeWithoutResult {
			try {
				scheduler.scheduleJob(job, trigger)
			} catch (e: ObjectAlreadyExistsException) {
				logger.warn("Failed to schedule stalled tasks monitoring job: ${e.message}")
			}
		}
	}
}

fun main(args: Array<String>) {
	runApplication<RunnerApplication>(*args)
}
