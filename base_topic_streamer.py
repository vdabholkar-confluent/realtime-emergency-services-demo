#!/usr/bin/env python3
"""
Kafka CSV Producer with Avro Schema
Streams CSV data to Kafka topics with Avro serialization
"""

import os
import json
import time
import logging
import pandas as pd
from typing import Dict, Any
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaCSVProducer:
    def __init__(self):
        """Initialize Kafka producer with Avro serialization"""
        
        # Kafka Configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY', ''),
            'sasl.password': os.getenv('KAFKA_API_SECRET', ''),
        }
        
        # Admin client configuration for topic creation
        self.admin_config = {
            'bootstrap.servers': self.kafka_config['bootstrap.servers'],
            'security.protocol': self.kafka_config['security.protocol'],
            'sasl.mechanisms': self.kafka_config['sasl.mechanisms'],
            'sasl.username': self.kafka_config['sasl.username'],
            'sasl.password': self.kafka_config['sasl.password'],
        }
        
        # Schema Registry Configuration
        self.schema_registry_config = {
            'url': '',
            'basic.auth.user.info': ''
        }
        
        # Initialize clients
        self.producer = Producer(self.kafka_config)
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
        
        # Initialize admin client for topic management
        from confluent_kafka.admin import AdminClient, NewTopic
        self.admin_client = AdminClient(self.admin_config)
        
        # Define topics
        self.topics = ['incidents_raw', 'personnel_raw', 'vehicles_raw']
        
        # Create topics if they don't exist
        self._create_topics_if_needed()
        
        # Define Avro schemas for each entity
        self.schemas = self._define_schemas()
        
        # Create serializers
        self.serializers = self._create_serializers()
        
        logger.info("Kafka CSV Producer initialized successfully")
    
    def _create_topics_if_needed(self):
        """Create Kafka topics if they don't exist"""
        from confluent_kafka.admin import NewTopic
        
        try:
            # Get existing topics
            cluster_metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(cluster_metadata.topics.keys())
            
            # Create topics that don't exist
            topics_to_create = []
            for topic_name in self.topics:
                if topic_name not in existing_topics:
                    logger.info(f"Creating topic: {topic_name}")
                    topic = NewTopic(
                        topic=topic_name,
                        num_partitions=3,  # 3 partitions for good parallelism
                        replication_factor=3  # 3 replicas for durability
                    )
                    topics_to_create.append(topic)
                else:
                    logger.info(f"Topic already exists: {topic_name}")
            
            if topics_to_create:
                # Create topics
                futures = self.admin_client.create_topics(topics_to_create)
                
                # Wait for topic creation to complete
                for topic, future in futures.items():
                    try:
                        future.result()  # The result itself is None
                        logger.info(f"✅ Topic created successfully: {topic}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.info(f"Topic already exists: {topic}")
                        else:
                            logger.error(f"Failed to create topic {topic}: {e}")
                            raise
            else:
                logger.info("All required topics already exist")
                
        except Exception as e:
            logger.error(f"Error managing topics: {e}")
            raise
    
    def _define_schemas(self) -> Dict[str, str]:
        """Define Avro schemas for each CSV entity"""
        
        # Incidents schema
        incidents_schema = {
            "type": "record",
            "name": "Incident",
            "namespace": "com.emergency.incidents",
            "fields": [
                {"name": "incident_id", "type": "int"},
                {"name": "incident_type", "type": "string"},
                {"name": "severity", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "description", "type": "string"},
                {"name": "personnel_required", "type": "int"},
                {"name": "vehicles_required", "type": "int"},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        
        # Personnel schema
        personnel_schema = {
            "type": "record",
            "name": "Personnel",
            "namespace": "com.emergency.personnel",
            "fields": [
                {"name": "personnel_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "department", "type": "string"},
                {"name": "rank", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "status", "type": "string"},
                {"name": "home_station", "type": "string"},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        
        # Vehicles schema
        vehicles_schema = {
            "type": "record",
            "name": "Vehicle",
            "namespace": "com.emergency.vehicles",
            "fields": [
                {"name": "vehicle_id", "type": "int"},
                {"name": "call_sign", "type": "string"},
                {"name": "vehicle_type", "type": "string"},
                {"name": "department", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "status", "type": "string"},
                {"name": "home_station", "type": "string"},
                {"name": "crew_capacity", "type": "int"},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        
        return {
            'incidents': json.dumps(incidents_schema),
            'personnel': json.dumps(personnel_schema),
            'vehicles': json.dumps(vehicles_schema)
        }
    
    def _create_serializers(self) -> Dict[str, AvroSerializer]:
        """Create Avro serializers for each schema"""
        serializers = {}
        
        for entity_type, schema_str in self.schemas.items():
            serializers[entity_type] = AvroSerializer(
                self.schema_registry_client,
                schema_str,
                self._to_dict
            )
        
        return serializers
    
    @staticmethod
    def _to_dict(obj, ctx):
        """Convert object to dictionary for Avro serialization"""
        if isinstance(obj, dict):
            return obj
        return obj.__dict__
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def produce_single_record(self, topic: str, entity_type: str, data: Dict):
        """Produce a single record to a Kafka topic"""
        try:
            # Serialize and send message
            serialized_value = self.serializers[entity_type](
                data,
                SerializationContext(topic, MessageField.VALUE)
            )
            
            self.producer.produce(
                topic=topic,
                key=str(data.get('incident_id') or data.get('personnel_id') or data.get('vehicle_id')),
                value=serialized_value,
                on_delivery=self._delivery_callback
            )
            
            # Flush after each message to ensure it's sent immediately
            self.producer.flush()
            
            logger.info(f"Produced {entity_type} record to {topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error producing record to {topic}: {e}")
            return False
    
    def stream_csv_data(self, incidents_file: str = 'incidents.csv', 
                       personnel_file: str = 'personnel.csv', 
                       vehicles_file: str = 'vehicles.csv',
                       interval: float = 1.0):
        """Stream CSV data to Kafka topics with one record per second"""
        logger.info(f"Starting to stream CSV data to Kafka topics (one record per {interval} second)")
        
        try:
            # Load data from CSV files
            incidents_df = pd.read_csv(incidents_file)
            personnel_df = pd.read_csv(personnel_file)
            vehicles_df = pd.read_csv(vehicles_file)
            
            # Find the maximum number of records across all files
            max_records = max(len(incidents_df), len(personnel_df), len(vehicles_df))
            # max_records = len(incidents_df)
            
            # Stream one record from each file per interval
            for i in range(max_records):
                current_time = pd.Timestamp.now().value // 1000000  # Current time in milliseconds
                
                # Send one incident record if available
                if i < len(incidents_df):
                    row = incidents_df.iloc[i]
                    incident_data = {
                        'incident_id': int(row['incident_id']),
                        'incident_type': str(row['incident_type']),
                        'severity': str(row['severity']),
                        'latitude': float(row['latitude']),
                        'longitude': float(row['longitude']),
                        'description': str(row['description']),
                        'personnel_required': int(row['personnel_required']),
                        'vehicles_required': int(row['vehicles_required']),
                        'timestamp': current_time
                    }
                    self.produce_single_record('incidents_raw', 'incidents', incident_data)
                
                Send one personnel record if available
                if i < len(personnel_df):
                    row = personnel_df.iloc[i]
                    personnel_data = {
                        'personnel_id': int(row['personnel_id']),
                        'name': str(row['name']),
                        'department': str(row['department']),
                        'rank': str(row['rank']),
                        'latitude': float(row['latitude']),
                        'longitude': float(row['longitude']),
                        'status': str(row['status']),
                        'home_station': str(row['home_station']),
                        'timestamp': current_time
                    }
                    self.produce_single_record('personnel_raw', 'personnel', personnel_data)
                
                # Send one vehicle record if available
                if i < len(vehicles_df):
                    row = vehicles_df.iloc[i]
                    vehicle_data = {
                        'vehicle_id': int(row['vehicle_id']),
                        'call_sign': str(row['call_sign']),
                        'vehicle_type': str(row['vehicle_type']),
                        'department': str(row['department']),
                        'latitude': float(row['latitude']),
                        'longitude': float(row['longitude']),
                        'status': str(row['status']),
                        'home_station': str(row['home_station']),
                        'crew_capacity': int(row['crew_capacity']),
                        'timestamp': current_time
                    }
                    self.produce_single_record('vehicles_raw', 'vehicles', vehicle_data)
                
                logger.info(f"✅ Sent record batch {i+1}/{max_records}")
                
                # Wait for the specified interval before sending the next batch
                time.sleep(interval)
            
            logger.info("✅ Successfully streamed all CSV data to Kafka topics")
            
        except Exception as e:
            logger.error(f"❌ Error streaming CSV data: {e}")
            raise
    
    def close(self):
        """Close producer and clean up resources"""
        try:
            self.producer.flush()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")

def main():
    """Main function to run the CSV producer"""
    producer = None
    
    try:
        logger.info("🚀 Starting Emergency Services CSV Producer - Streaming Mode")
        
        # Initialize producer
        producer = KafkaCSVProducer()
        
        # Stream CSV data one record at a time
        producer.stream_csv_data(interval=1.0)  # One record per second
        
        logger.info("🎉 CSV data streaming completed successfully")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Producer stopped by user")
        
    except Exception as e:
        logger.error(f"❌ Producer failed: {e}")
        raise
        
    finally:
        if producer:
            producer.close()
        logger.info("👋 Producer shutdown complete")

if __name__ == "__main__":
    main()
