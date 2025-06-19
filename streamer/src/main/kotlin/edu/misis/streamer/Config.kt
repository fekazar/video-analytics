package edu.misis.streamer

import io.minio.MinioClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class Config {
    @Bean
    fun s3Client(
        @Value("\${s3.minio.endpoint}") minioEndpoint: String
    ): MinioClient {
        return MinioClient.builder()
            .endpoint(minioEndpoint)
            .credentials("minioadmin", "minioadmin")
            .build()
    }

    @Bean
    fun consumerConfigs(
        @Value("\${spring.kafka.bootstrap-servers}") bootstrapServer: String
    ) = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServer,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
    )

    @Bean
    fun streamStateMachineContainerFactory(
        consumerProperties: Map<String, Any?>
    ): ConcurrentKafkaListenerContainerFactory<String, StreamEventData> {
        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerProperties,
            StringDeserializer(),
            JsonDeserializer(StreamEventData::class.java)
        )
        return ConcurrentKafkaListenerContainerFactory<String, StreamEventData>().also {
            it.consumerFactory = consumerFactory
        }
    }

    @Bean
    fun inferenceResultsContainerFactory(
        consumerProperties: Map<String, Any?>
    ): ConcurrentKafkaListenerContainerFactory<String, InferenceResultData> {
        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerProperties
                    + (ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100)
                    + (ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 5120),
            StringDeserializer(),
            JsonDeserializer(InferenceResultData::class.java)
        )
        return ConcurrentKafkaListenerContainerFactory<String, InferenceResultData>().also {
            it.consumerFactory = consumerFactory
            it.isBatchListener = true
        }
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