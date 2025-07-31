# dashboard/app.py - Kafka Stream Version
import os
import json
import threading
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import logging

# Kafka imports
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'emergency-services-demo-key'
socketio = SocketIO(app, cors_allowed_origins="*")

class KafkaDataManager:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY', ''),
            'sasl.password': os.getenv('KAFKA_API_SECRET', ''),
            'group.id': 'emergency-dashboard-group-1',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }

        self.schema_registry_config = {
            'url': '',
            'basic.auth.user.info': ''
        }

        # Topics to consume
        self.topics = [
            'active_routes_personnel',
            'active_routes_vehicle', 
            'personnel_risk_status',
            'resource_deployment',
            'situational_awareness'
        ]

        # In-memory data storage
        self.data_store = {
            'incidents': [],
            'personnel': [],
            'vehicles': [],
            'routes_personnel': [],
            'routes_vehicle': [],
            'alerts': []
        }
        
        # Lock for thread-safe access
        self.data_lock = threading.Lock()
        
        # Initialize schema registry and deserializers
        self.setup_kafka()
        
    def setup_kafka(self):
        """Initialize Kafka consumer and schema registry"""
        try:
            # Setup schema registry client
            self.schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
            
            # Setup deserializers for each topic
            self.deserializers = {}
            for topic in self.topics:
                try:
                    # Get latest schema for topic value
                    latest_schema = self.schema_registry_client.get_latest_version(f"{topic}-value")
                    avro_deserializer = AvroDeserializer(
                        self.schema_registry_client,
                        latest_schema.schema.schema_str
                    )
                    self.deserializers[topic] = avro_deserializer
                    logger.info(f"Setup deserializer for topic: {topic}")
                except Exception as e:
                    logger.error(f"Failed to setup deserializer for {topic}: {e}")
            
            # Setup consumer
            self.consumer = Consumer(self.kafka_config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Subscribed to topics: {self.topics}")
            
        except Exception as e:
            logger.error(f"Failed to setup Kafka: {e}")
            raise
    
    def start_consuming(self):
        """Start consuming messages from Kafka topics"""
        logger.info("Starting Kafka consumer...")
        
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Process the message
                self.process_message(msg)
                
            except Exception as e:
                logger.error(f"Error consuming message: {e}")
                time.sleep(1)
    
    def process_message(self, msg):
        """Process incoming Kafka message"""
        topic = msg.topic()
        
        try:
            # Deserialize the message
            if topic in self.deserializers:
                ctx = SerializationContext(topic, MessageField.VALUE)
                deserialized_data = self.deserializers[topic](msg.value(), ctx)
                
                # Update data store based on topic
                with self.data_lock:
                    self.update_data_store(topic, deserialized_data)
                    
                logger.debug(f"Processed message from {topic}")
            else:
                logger.warning(f"No deserializer found for topic: {topic}")
                
        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}")
    
    def update_data_store(self, topic, data):
        """Update in-memory data store with new message data"""
        if not data:
            return
            
        try:
            if topic == 'situational_awareness':
                # Handle incidents data
                self.update_incidents(data)
                
            elif topic == 'personnel_risk_status':
                # Handle personnel data
                self.update_personnel(data)
                
            elif topic == 'resource_deployment':
                # Handle vehicle data
                self.update_vehicles(data)
                
            elif topic == 'active_routes_personnel':
                # Handle personnel routes
                self.update_routes_personnel(data)
                
            elif topic == 'active_routes_vehicle':
                # Handle vehicle routes
                self.update_routes_vehicle(data)
                
        except Exception as e:
            logger.error(f"Error updating data store for {topic}: {e}")
    
    def update_incidents(self, data):
        """Update incidents data"""
        # Convert Avro data to dict format expected by frontend
        incident = {
            'incident_id': data.get('incident_id'),
            'incident_type': data.get('incident_type'),
            'severity': data.get('severity'),
            'incident_lat': float(data['incident_lat']) if data.get('incident_lat') else -33.8688,
            'incident_lng': float(data['incident_lng']) if data.get('incident_lng') else 151.2093,
            'incident_status': data.get('incident_status'),
            'description': data.get('description'),
            'assigned_personnel_count': data.get('assigned_personnel_count', 0),
            'assigned_vehicle_count': data.get('assigned_vehicle_count', 0),
            'created_at': data.get('created_at').isoformat() if data.get('created_at') else None,
            'updated_at': data.get('updated_at').isoformat() if data.get('updated_at') else None
        }
        
        # Update or add incident
        existing_idx = next((i for i, inc in enumerate(self.data_store['incidents']) 
                           if inc.get('incident_id') == incident['incident_id']), None)
        
        if existing_idx is not None:
            self.data_store['incidents'][existing_idx] = incident
        else:
            self.data_store['incidents'].append(incident)
            
        # Keep only last 100 incidents
        self.data_store['incidents'] = self.data_store['incidents'][-100:]
    
    def update_personnel(self, data):
        """Update personnel data"""
        personnel = {
            'personnel_id': data.get('personnel_id'),
            'name': data.get('name'),
            'department': data.get('department'),
            'rank': data.get('rank'),
            'latitude': float(data['latitude']) if data.get('latitude') else -33.8688,
            'longitude': float(data['longitude']) if data.get('longitude') else 151.2093,
            'location_status': data.get('location_status', 'standby'),
            'heart_rate': data.get('heart_rate', 75),
            'oxygen_saturation': data.get('oxygen_saturation', 98),
            'body_temperature': float(data['body_temperature']) if data.get('body_temperature') else 36.5,
            'risk_level': data.get('risk_level', 'NORMAL'),
            'home_station': data.get('home_station'),
            'assigned_incident': data.get('assigned_incident'),
            'distance_to_incident': float(data['distance_to_incident']) if data.get('distance_to_incident') else 0,
            'movement_progress': float(data['movement_progress']) if data.get('movement_progress') else 0
        }
        
        # Update or add personnel
        existing_idx = next((i for i, p in enumerate(self.data_store['personnel']) 
                           if p.get('personnel_id') == personnel['personnel_id']), None)
        
        if existing_idx is not None:
            self.data_store['personnel'][existing_idx] = personnel
        else:
            self.data_store['personnel'].append(personnel)
            
        # Check for critical alerts
        self.check_personnel_alerts(personnel)
    
    def update_vehicles(self, data):
        """Update vehicle data"""
        vehicle = {
            'vehicle_id': data.get('vehicle_id'),
            'call_sign': data.get('call_sign'),
            'vehicle_type': data.get('vehicle_type'),
            'department': data.get('department'),
            'latitude': float(data['lat_grid']) if data.get('lat_grid') else -33.8688,
            'longitude': float(data['lng_grid']) if data.get('lng_grid') else 151.2093,
            'status': data.get('status'),
            'home_station': data.get('home_station'),
            'crew_capacity': data.get('crew_capacity'),
            'assigned_incident': data.get('assigned_incident'),
            'distance_to_incident': float(data['distance_to_incident']) if data.get('distance_to_incident') else 0,
            'movement_progress': float(data['movement_progress']) if data.get('movement_progress') else 0,
            'total_vehicles': data.get('total_vehicles', 0),
            'available_vehicles': data.get('available_vehicles', 0),
            'enroute_vehicles': data.get('enroute_vehicles', 0),
            'deployed_vehicles': data.get('deployed_vehicles', 0)
        }
        
        # Update or add vehicle
        existing_idx = next((i for i, v in enumerate(self.data_store['vehicles']) 
                           if v.get('call_sign') == vehicle['call_sign']), None)
        
        if existing_idx is not None:
            self.data_store['vehicles'][existing_idx] = vehicle
        else:
            self.data_store['vehicles'].append(vehicle)
    
    def update_routes_personnel(self, data):
        """Update personnel routes data"""
        route = {
            'personnel_id': data.get('personnel_id'),
            'vehicle_id': None,
            'current_lat': float(data['current_lat']) if data.get('current_lat') else 0,
            'current_lng': float(data['current_lng']) if data.get('current_lng') else 0,
            'destination_lat': float(data['destination_lat']) if data.get('destination_lat') else 0,
            'destination_lng': float(data['destination_lng']) if data.get('destination_lng') else 0,
            'incident_id': data.get('incident_id'),
            'incident_type': data.get('incident_type'),
            'severity': data.get('severity'),
            'distance_km': float(data['distance_km']) if data.get('distance_km') else 0,
            'eta_minutes': data.get('eta_minutes', 0),
            'personnel_status': data.get('personnel_status'),
            'progress_percent': float(data['progress_percent']) if data.get('progress_percent') else 0,
            'route_type': 'personnel'
        }
        
        # Update routes personnel list
        existing_idx = next((i for i, r in enumerate(self.data_store['routes_personnel']) 
                           if r.get('personnel_id') == route['personnel_id']), None)
        
        if existing_idx is not None:
            self.data_store['routes_personnel'][existing_idx] = route
        else:
            self.data_store['routes_personnel'].append(route)
    
    def update_routes_vehicle(self, data):
        """Update vehicle routes data"""
        route = {
            'personnel_id': None,
            'vehicle_id': data.get('vehicle_id'),
            'current_lat': float(data['current_lat']) if data.get('current_lat') else 0,
            'current_lng': float(data['current_lng']) if data.get('current_lng') else 0,
            'destination_lat': float(data['destination_lat']) if data.get('destination_lat') else 0,
            'destination_lng': float(data['destination_lng']) if data.get('destination_lng') else 0,
            'incident_id': data.get('incident_id'),
            'incident_type': data.get('incident_type'),
            'severity': data.get('severity'),
            'distance_km': float(data['distance_km']) if data.get('distance_km') else 0,
            'eta_minutes': data.get('eta_minutes', 0),
            'personnel_status': data.get('personnel_status'),
            'progress_percent': float(data['progress_percent']) if data.get('progress_percent') else 0,
            'route_type': 'vehicle'
        }
        
        # Update routes vehicle list
        existing_idx = next((i for i, r in enumerate(self.data_store['routes_vehicle']) 
                           if r.get('vehicle_id') == route['vehicle_id']), None)
        
        if existing_idx is not None:
            self.data_store['routes_vehicle'][existing_idx] = route
        else:
            self.data_store['routes_vehicle'].append(route)
    
    def check_personnel_alerts(self, personnel):
        """Check for critical personnel conditions and generate alerts"""
        alerts = []
        
        # Check heart rate
        heart_rate = personnel.get('heart_rate', 75)
        if heart_rate > 120:
            alerts.append({
                'personnel_id': personnel['personnel_id'],
                'latitude': personnel['latitude'],
                'longitude': personnel['longitude'],
                'heart_rate': heart_rate,
                'oxygen_saturation': personnel.get('oxygen_saturation'),
                'body_temperature': personnel.get('body_temperature'),
                'recommendation': f'High heart rate detected: {heart_rate} bpm. Monitor closely.',
                'alert_timestamp': datetime.now().isoformat()
            })
        
        # Check oxygen saturation
        oxygen = personnel.get('oxygen_saturation', 98)
        if oxygen < 95:
            alerts.append({
                'personnel_id': personnel['personnel_id'],
                'latitude': personnel['latitude'],
                'longitude': personnel['longitude'],
                'heart_rate': personnel.get('heart_rate'),
                'oxygen_saturation': oxygen,
                'body_temperature': personnel.get('body_temperature'),
                'recommendation': f'Low oxygen saturation: {oxygen}%. Immediate attention required.',
                'alert_timestamp': datetime.now().isoformat()
            })
        
        # Check body temperature
        temp = personnel.get('body_temperature', 36.5)
        if temp > 38.0 or temp < 35.0:
            alerts.append({
                'personnel_id': personnel['personnel_id'],
                'latitude': personnel['latitude'],
                'longitude': personnel['longitude'],
                'heart_rate': personnel.get('heart_rate'),
                'oxygen_saturation': personnel.get('oxygen_saturation'),
                'body_temperature': temp,
                'recommendation': f'Abnormal body temperature: {temp}°C. Check for heat stress or hypothermia.',
                'alert_timestamp': datetime.now().isoformat()
            })
        
        # Add alerts to store
        for alert in alerts:
            self.data_store['alerts'].append(alert)
            # Keep only last 50 alerts
            self.data_store['alerts'] = self.data_store['alerts'][-50:]
    
    def get_dashboard_data(self):
        """Get current dashboard data in thread-safe manner"""
        with self.data_lock:
            # Combine personnel and vehicle routes for the frontend
            all_routes = self.data_store['routes_personnel'] + self.data_store['routes_vehicle']
            
            return {
                'incidents': list(self.data_store['incidents']),
                'personnel': list(self.data_store['personnel']),
                'vehicles': list(self.data_store['vehicles']),
                'routes': all_routes,
                'alerts': list(self.data_store['alerts'][-10:]),  # Last 10 alerts
                'timestamp': datetime.now().isoformat()
            }
    
    def get_dashboard_stats(self):
        """Calculate dashboard statistics"""
        with self.data_lock:
            # Incident stats by severity
            incidents_by_severity = defaultdict(int)
            for incident in self.data_store['incidents']:
                severity = incident.get('severity', 'unknown')
                incidents_by_severity[severity] += 1
            
            # Personnel stats by risk level
            personnel_by_risk = defaultdict(int)
            for person in self.data_store['personnel']:
                risk = person.get('risk_level', 'UNKNOWN')
                personnel_by_risk[risk] += 1
            
            # Vehicle stats
            total_available = sum(v.get('available_vehicles', 0) for v in self.data_store['vehicles'])
            total_enroute = sum(v.get('enroute_vehicles', 0) for v in self.data_store['vehicles'])
            total_deployed = sum(v.get('deployed_vehicles', 0) for v in self.data_store['vehicles'])
            
            return {
                'incidents': dict(incidents_by_severity),
                'personnel': dict(personnel_by_risk),
                'vehicles': {
                    'available': total_available,
                    'enroute': total_enroute,
                    'deployed': total_deployed
                },
                'timestamp': datetime.now().isoformat()
            }

# Initialize Kafka data manager
kafka_manager = KafkaDataManager()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/incidents')
def get_incidents():
    """Get current incidents"""
    data = kafka_manager.get_dashboard_data()
    return jsonify(data['incidents'])

@app.route('/api/personnel')
def get_personnel():
    """Get current personnel locations and status"""
    data = kafka_manager.get_dashboard_data()
    return jsonify(data['personnel'])

@app.route('/api/vehicles')
def get_vehicles():
    """Get current vehicle locations"""
    data = kafka_manager.get_dashboard_data()
    return jsonify(data['vehicles'])

@app.route('/api/routes')
def get_active_routes():
    """Get active response routes"""
    data = kafka_manager.get_dashboard_data()
    return jsonify(data['routes'])

@app.route('/api/critical-alerts')
def get_critical_alerts():
    """Get critical personnel alerts"""
    data = kafka_manager.get_dashboard_data()
    return jsonify(data['alerts'])

@app.route('/api/dashboard-stats')
def get_dashboard_stats():
    """Get dashboard statistics"""
    return jsonify(kafka_manager.get_dashboard_stats())

def broadcast_updates():
    """Continuously broadcast updates to connected clients"""
    while True:
        try:
            # Get latest data from Kafka manager
            data = kafka_manager.get_dashboard_data()
            stats = kafka_manager.get_dashboard_stats()
            
            logger.info(f"Broadcasting update: {len(data['incidents'])} incidents, "
                       f"{len(data['personnel'])} personnel, {len(data['vehicles'])} vehicles, "
                       f"{len(data['routes'])} routes")
            
            # Debug: Log first incident if exists
            if data['incidents']:
                logger.info(f"Sample incident: {data['incidents'][0]}")
            
            # Debug: Log first vehicle if exists  
            if data['vehicles']:
                logger.info(f"Sample vehicle: {data['vehicles'][0]}")
            
            # Broadcast to all connected clients
            socketio.emit('data_update', {
                'incidents': data['incidents'],
                'personnel': data['personnel'],
                'vehicles': data['vehicles'],
                'routes': data['routes'],
                'alerts': data['alerts'],
                'stats': stats,
                'timestamp': data['timestamp']
            })
            
            # Check for new critical alerts
            if data['alerts']:
                socketio.emit('critical_alert', data['alerts'][0])
            
            time.sleep(2)  # Update every 2 seconds for real-time feel
            
        except Exception as e:
            logger.error(f"Error in broadcast_updates: {e}")
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(5)

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info('Client connected')
    emit('status', {'msg': 'Connected to Emergency Services Dashboard'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info('Client disconnected')

@socketio.on('request_update')
def handle_update_request():
    """Handle manual update request from client"""
    try:
        data = kafka_manager.get_dashboard_data()
        stats = kafka_manager.get_dashboard_stats()
        
        logger.info(f"Manual update request: {len(data['vehicles'])} vehicles being sent")
        
        # Debug: Log sample data
        if data['incidents']:
            logger.info(f"Sample incident data: {data['incidents'][0]}")
        if data['vehicles']:
            logger.info(f"Sample vehicle data: {data['vehicles'][0]}")
        
        emit('data_update', {
            'incidents': data['incidents'],
            'personnel': data['personnel'],
            'vehicles': data['vehicles'],
            'routes': data['routes'],
            'alerts': data['alerts'],
            'stats': stats,
            'timestamp': data['timestamp']
        })
    except Exception as e:
        logger.error(f"Error handling update request: {e}")
        import traceback
        logger.error(traceback.format_exc())
        emit('error', {'msg': 'Failed to fetch updates'})

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    kafka_thread = threading.Thread(target=kafka_manager.start_consuming, daemon=True)
    kafka_thread.start()
    
    # Start background thread for broadcasting updates
    update_thread = threading.Thread(target=broadcast_updates, daemon=True)
    update_thread.start()
    
    # Run the Flask app with SocketIO
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
