package edu.misis.runner

import io.minio.MinioClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
class Config {
    @Bean
    fun s3Client(): MinioClient {
        return MinioClient.builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin")
            .build()
    }

    @Bean
    fun streamStateMachineTopic() = TopicBuilder.name(STREAM_STATE_MACHINE_EVENTS_TOPIC)
        .partitions(6)
        .build()

    @Bean
    fun inferenceChunksTopic() = TopicBuilder.name(CHUNKS_FOR_INFERENCE_TOPIC)
        .partitions(6)
        .build()
}