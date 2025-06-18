from confluent_kafka import Consumer, Producer, KafkaError
import json
from loguru import logger
from config import kafka_config
import sys
import cv2
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Dict, Any
import io

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

class VideoAnalyticsService:
    def __init__(self):
        # Initialize Kafka consumer
        consumer_config = {
            'bootstrap.servers': kafka_config.bootstrap_servers,
            'group.id': kafka_config.consumer_group,
            'auto.offset.reset': kafka_config.auto_offset_reset,
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([kafka_config.topic])
        
        # Initialize Kafka producer
        producer_config = {
            'bootstrap.servers': kafka_config.bootstrap_servers
        }
        self.producer = Producer(producer_config)
        
        # Load the pre-trained face detection model
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        logger.info(f"Initialized Kafka consumer for topic: {kafka_config.topic}")

    def calculate_frame_timestamp(self, base_timestamp: datetime, fps: int, frame_number: int) -> float:
        """Calculate timestamp for a specific frame based on base timestamp and FPS."""
        frame_duration = timedelta(seconds=1/fps)
        frame_timestamp = base_timestamp + (frame_duration * frame_number)
        return frame_timestamp.timestamp()

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_inference_result(self, stream_id: str, frame_number: int, frame_timestamp: float, faces_count: int, stream_url: str, pre_shared_url: str):
        """Send inference result to Kafka topic."""
        result = {
            "timestamp": frame_timestamp,
            "facesCount": faces_count
        }
        
        try:
            # Convert stream_id to bytes for the key
            key_bytes = stream_id.encode('utf-8') if stream_id else None
            # Serialize result to JSON bytes
            value_bytes = json.dumps(result).encode('utf-8')
            
            self.producer.produce(
                "inference-results",
                key=key_bytes,
                value=value_bytes,
                callback=self.delivery_report
            )
            # Trigger any available delivery report callbacks
            self.producer.poll(0)
            logger.debug(f"Sent inference result for stream {stream_id}, frame {frame_number}")
        except Exception as e:
            logger.error(f"Failed to send inference result: {str(e)}")

    def download_video_chunk(self, url: str) -> bytes:
        """Download video chunk from the pre-shared URL."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to download video chunk from {url}: {str(e)}")
            raise

    def process_frame(self, frame: np.ndarray) -> int:
        """Process a single frame and return the number of faces detected."""
        # Convert to grayscale for face detection
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # Detect faces
        faces = self.face_cascade.detectMultiScale(
            gray,
            scaleFactor=1.1,
            minNeighbors=5,
            minSize=(30, 30)
        )
        
        return len(faces)

    def parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string in format 'seconds.fraction'."""
        try:
            # Convert string to float and then to datetime
            timestamp_float = float(timestamp_str)
            return datetime.fromtimestamp(timestamp_float)
        except ValueError as e:
            logger.error(f"Failed to parse timestamp {timestamp_str}: {str(e)}")
            raise

    def process_video_chunk(self, message):
        """
        Process a video chunk received from Kafka.
        Counts faces in each frame of the video chunk and sends results to Kafka.
        """
        try:
            # Extract video chunk data and stream ID
            stream_id = message.key().decode('utf-8') if message.key() else None
            # Manually deserialize JSON value
            video_data = json.loads(message.value().decode('utf-8')) if message.value() else None
            if not video_data:
                raise ValueError("Received empty message value")
                
            stream_url = video_data['streamUrl']
            logger.info(f"Processing video chunk from stream: {stream_id}")
            logger.info(f"Video data: {video_data}")
            
            # Parse the creation timestamp (seconds.fraction format)
            created_at = self.parse_timestamp(video_data['createdAt'])
            fps = video_data['fps']
            
            # Create video capture from memory file
            cap = cv2.VideoCapture()
            ret = cap.open(video_data['preSharedUrl'])
            
            if not ret or not cap.isOpened():
                raise ValueError("Failed to open video chunk")
            
            # Get total number of frames
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            frame_count = 0
            total_faces = 0
            
            # Process each frame
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                faces_in_frame = self.process_frame(frame)
                total_faces += faces_in_frame
                
                # Calculate frame timestamp
                frame_timestamp = self.calculate_frame_timestamp(created_at, fps, frame_count)
                
                # Send inference result to Kafka
                self.send_inference_result(stream_id, frame_count, frame_timestamp, faces_in_frame, stream_url, video_data['preSharedUrl'])
                
                frame_count += 1
                logger.debug(f"Frame {frame_count}/{total_frames}: detected {faces_in_frame} faces")
            
            # Calculate average faces per frame
            avg_faces = total_faces / frame_count if frame_count > 0 else 0
            
            # Log results
            logger.info(f"Video chunk analysis complete for stream {stream_id}:")
            logger.info(f"- Total frames processed: {frame_count}")
            logger.info(f"- Total faces detected: {total_faces}")
            logger.info(f"- Average faces per frame: {avg_faces:.2f}")
            
            # Flush producer to ensure all messages are sent
            self.producer.flush()
            
            cap.release()
            
        except Exception as e:
            logger.error(f"Error processing video chunk: {str(e)}")
            raise

    def run(self):
        """
        Main service loop that consumes messages from Kafka.
        """
        logger.info("Starting video analytics service...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                logger.debug(f"Received message at offset {msg.offset()}")
                self.process_video_chunk(msg)
                
        except KeyboardInterrupt:
            logger.info("Shutting down video analytics service...")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("Kafka consumer and producer closed")

if __name__ == "__main__":
    service = VideoAnalyticsService()
    service.run() 