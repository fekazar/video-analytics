package edu.misis.runner

import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.net.url.Url
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
class Config {
    fun s3Client(): S3Client {
        return S3Client {
            endpointUrl = Url.parse("s3://localhost:9000")
        }
    }
}