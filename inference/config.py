from dataclasses import dataclass

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:29092"
    consumer_group: str = "inference-group"
    topic: str = "chunks-for-inference"
    auto_offset_reset: str = "earliest"

kafka_config = KafkaConfig() 