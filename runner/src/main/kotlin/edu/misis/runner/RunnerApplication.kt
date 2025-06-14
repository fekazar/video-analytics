package edu.misis.runner

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RunnerApplication

fun main(args: Array<String>) {
	runApplication<RunnerApplication>(*args)
}
