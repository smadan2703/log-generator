#!/usr/bin/env python3
"""
Vector Log Generator - FIXED VERSION with working Blueprint
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Blueprint
import logging
import logging.handlers
import sys
import os
import threading
import hashlib
import queue

app = Flask(__name__)

# Configure dual logging: stdout AND file
def setup_dual_logging():
    """Setup logging to both stdout and file"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter (just the JSON message)
    formatter = logging.Formatter('%(message)s')
    
    # Handler 1: stdout (for Vector container log collection)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    
    # Handler 2: file (for Vector file source)
    log_dir = os.environ.get('LOG_DIR', '/var/log/app')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'exper.log')
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, 
        maxBytes=500*1024*1024,  # 500MB per file
        backupCount=5            # Keep 5 backup files
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger, log_file

logger, log_file_path = setup_dual_logging()

# Global sequence tracking
class SequenceTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.sequence_number = 0
        self.session_id = uuid.uuid4().hex[:8]
        self.instance_id = os.environ.get('HOSTNAME', 'unknown')[:8]
        self.start_time = datetime.now()
        
    def get_next_sequence(self):
        with self.lock:
            self.sequence_number += 1
            return self.sequence_number
    
    def get_session_info(self):
        return {
            'session_id': self.session_id,
            'instance_id': self.instance_id,
            'start_time': self.start_time.isoformat()
        }

# Global tracker instance
sequence_tracker = SequenceTracker()

# Background task management
background_tasks = {}

class BackgroundLoadTest:
    def __init__(self, task_id, duration, rate, session_id):
        self.task_id = task_id
        self.duration = duration
        self.rate = rate
        self.session_id = session_id
        self.start_time = datetime.now()
        self.logs_generated = 0
        self.status = "running"
        self.error = None
        
    def run(self):
        """Run the load test in background"""
        try:
            logger.info(json.dumps({
                "background_task": "started",
                "task_id": self.task_id,
                "duration": self.duration,
                "rate": self.rate,
                "session_id": self.session_id,
                "started_at": self.start_time.isoformat()
            }, separators=(',', ':')))
            
            end_time = self.start_time + timedelta(seconds=self.duration)
            
            while datetime.now() < end_time and self.status == "running":
                # Generate logs for 1 second
                for _ in range(self.rate):
                    if datetime.now() >= end_time or self.status != "running":
                        break
                        
                    log_entry = generate_hotel_booking_log()
                    logger.info(json.dumps(log_entry, separators=(',', ':')))
                    self.logs_generated += 1
                
                # Sleep for rate limiting (1 second)
                if self.status == "running":
                    time.sleep(1.0)
            
            self.status = "completed"
            
            logger.info(json.dumps({
                "background_task": "completed",
                "task_id": self.task_id,
                "logs_generated": self.logs_generated,
                "duration_actual": (datetime.now() - self.start_time).total_seconds(),
                "completed_at": datetime.now().isoformat()
            }, separators=(',', ':')))
            
        except Exception as e:
            self.status = "error"
            self.error = str(e)
            logger.error(json.dumps({
                "background_task": "error",
                "task_id": self.task_id,
                "error": str(e),
                "logs_generated": self.logs_generated
            }, separators=(',', ':')))
    
    def stop(self):
        self.status = "stopped"
    
    def get_status(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        progress = min(elapsed / self.duration * 100, 100) if self.duration > 0 else 0
        
        return {
            "task_id": self.task_id,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "duration": self.duration,
            "rate": self.rate,
            "logs_generated": self.logs_generated,
            "elapsed_seconds": elapsed,
            "progress_percent": round(progress, 2),
            "estimated_completion": (self.start_time + timedelta(seconds=self.duration)).isoformat() if self.status == "running" else None,
            "error": self.error
        }

# Sample data for generating realistic logs
PROVIDERS = [
    {"code": "agoda.com", "name": "Agoda"},
    {"code": "booking.com", "name": "Booking.com"},
    {"code": "expedia.com", "name": "Expedia"},
    {"code": "hotels.com", "name": "Hotels.com"}
]

CURRENCIES = ["AED", "USD", "EUR", "GBP", "SGD"]
SITES = ["AE", "US", "SG", "UK", "DE"]
LOCALES = ["en", "ar", "de", "fr"]

def generate_search_id():
    """Generate a realistic search ID"""
    return f"{uuid.uuid4().hex[:16]}:{random.choice(['agoda.com', 'booking.com'])}"

def generate_hotel_booking_log():
    """Generate a hotel booking log entry with tracking fields"""
    search_id = generate_search_id()
    hotel_id = str(random.randint(100000, 999999))
    provider_hotel_id = str(random.randint(10000, 99999))
    provider = random.choice(PROVIDERS)
    currency = random.choice(CURRENCIES)
    site_code = random.choice(SITES)
    
    # Generate sequence and tracking information
    sequence_num = sequence_tracker.get_next_sequence()
    session_info = sequence_tracker.get_session_info()
    timestamp = datetime.now()
    
    # Create unique tracking ID (for loss detection)
    tracking_data = f"{session_info['session_id']}:{session_info['instance_id']}:{sequence_num}:{timestamp.isoformat()}"
    tracking_hash = hashlib.md5(tracking_data.encode()).hexdigest()[:16]
    
    # Generate realistic price data
    base_amount = round(random.uniform(200, 1000), 2)
    tax_amount = round(base_amount * random.uniform(0.15, 0.25), 2)
    total_amount = base_amount + tax_amount
    
    # Currency conversion
    usd_rate = 0.272
    amount_usd = round(base_amount * usd_rate, 8)
    tax_amount_usd = round(tax_amount * usd_rate, 8)
    total_amount_usd = round(total_amount * usd_rate, 8)
    
    # Generate check-in/out dates
    check_in = timestamp + timedelta(days=random.randint(1, 365))
    check_out = check_in + timedelta(days=random.randint(1, 14))
    
    log_entry = {
        # === TRACKING FIELDS FOR LOG LOSS DETECTION ===
        "_tracking": {
            "sequence_number": sequence_num,
            "session_id": session_info['session_id'],
            "instance_id": session_info['instance_id'],
            "tracking_hash": tracking_hash,
            "generated_at": timestamp.isoformat(),
            "generator_version": "1.1.0"
        },
        
        # === ORIGINAL HOTEL BOOKING DATA ===
        "id": f"{search_id}:{hotel_id}:{uuid.uuid4().hex[:8]}:{random.randint(50, 99)}",
        "search_id": search_id.split(':')[0],
        "hotel_id": hotel_id,
        "provider_hotel_id": provider_hotel_id,
        "params": "{}",
        "ecpc_id": uuid.uuid4().hex[:15],
        "room_ids": [uuid.uuid4().hex[:16]],
        "remaining_rooms_count": None,
        "remaining_rooms_count_map": [],
        "price": {
            "amount": base_amount,
            "amount_usd": amount_usd,
            "currency_code": currency,
            "base_amount": base_amount,
            "base_amount_usd": amount_usd,
            "base_currency_code": currency,
            "tax_amount": tax_amount,
            "tax_amount_usd": tax_amount_usd,
            "tax_inclusive": True,
            "total_amount": total_amount * 10,
            "total_amount_usd": total_amount_usd * 10,
            "total_tax_amount_usd": tax_amount_usd * 10,
            "url": f"https://www.{provider['code']}/partnersmw/partnersearch.aspx?cid={random.randint(1000000, 9999999)}&hid={provider_hotel_id}",
            "ecpc": 0.4,
            "local_tax_amount_usd": -2.0,
            "total_local_tax_amount_usd": -2.0,
            "amount_per_night": base_amount,
            "amount_per_night_usd": amount_usd,
            "tax_amount_per_night": tax_amount,
            "tax_amount_per_night_usd": tax_amount_usd
        },
        "provider": provider,
        "unpaid_fee": {
            "name": "local_tax",
            "amount": -2.0,
            "currency_code": currency,
            "amount_usd": -2.0
        },
        "site_code": site_code,
        "locale": random.choice(LOCALES),
        "rooms_count": random.randint(1, 3),
        "check_in": check_in.strftime("%Y-%m-%d"),
        "check_out": check_out.strftime("%Y-%m-%d"),
        "type": "DEFAULT",
        "is_mobile": random.choice([True, False]),
        "created_at": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "created_at_timezone": "+08:00",
        "bow_rate_id": None,
        "bow_hashed_rate_id": None,
        "rate_amenity_types": None
    }
    
    return log_entry

# Create blueprint with namespace - MUST be before route definitions
log_generator_bp = Blueprint('log_generator', __name__, url_prefix='/log-generator')

# === BLUEPRINT ROUTES ===

@log_generator_bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "namespace": "log-generator", "timestamp": datetime.now().isoformat()})

@log_generator_bp.route('/generate', methods=['GET'])
def generate_single_log():
    log_entry = generate_hotel_booking_log()
    logger.info(json.dumps(log_entry, separators=(',', ':')))
    
    return jsonify({
        "status": "log_generated",
        "timestamp": datetime.now().isoformat(),
        "log_entry": log_entry
    })

@log_generator_bp.route('/generate/bulk', methods=['POST'])
def generate_bulk_logs():
    data = request.get_json() or {}
    count = min(data.get('count', 10), 1000)
    rate_per_second = data.get('rate_per_second', 10)
    
    logs_generated = []
    
    for i in range(count):
        log_entry = generate_hotel_booking_log()
        logger.info(json.dumps(log_entry, separators=(',', ':')))
        logs_generated.append(log_entry['id'])
        
        if i < count - 1:
            time.sleep(1.0 / rate_per_second)
    
    return jsonify({
        "status": "bulk_logs_generated",
        "count": len(logs_generated),
        "rate_per_second": rate_per_second,
        "timestamp": datetime.now().isoformat(),
        "log_ids": logs_generated[:10]
    })

@log_generator_bp.route('/load-test/start', methods=['POST'])
def start_background_load_test():
    data = request.get_json() or {}
    duration_seconds = min(data.get('duration', 300), 7200)
    rate_per_second = min(data.get('rate', 100), 2000)
    
    task_id = f"task_{uuid.uuid4().hex[:8]}_{int(time.time())}"
    session_info = sequence_tracker.get_session_info()
    
    task = BackgroundLoadTest(
        task_id=task_id,
        duration=duration_seconds,
        rate=rate_per_second,
        session_id=session_info['session_id']
    )
    
    background_tasks[task_id] = task
    
    thread = threading.Thread(target=task.run, daemon=True)
    thread.start()
    
    return jsonify({
        "status": "background_task_started",
        "task_id": task_id,
        "duration": duration_seconds,
        "rate": rate_per_second,
        "session_id": session_info['session_id'],
        "estimated_logs": duration_seconds * rate_per_second,
        "estimated_completion": (datetime.now() + timedelta(seconds=duration_seconds)).isoformat(),
        "monitor_endpoint": f"/log-generator/load-test/status/{task_id}",
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/load-test/status/<task_id>', methods=['GET'])
def get_background_task_status(task_id):
    if task_id not in background_tasks:
        return jsonify({"error": "Task not found", "task_id": task_id}), 404
    
    task = background_tasks[task_id]
    return jsonify({
        "task_status": task.get_status(),
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/load-test/stop/<task_id>', methods=['POST'])
def stop_background_task(task_id):
    if task_id not in background_tasks:
        return jsonify({"error": "Task not found", "task_id": task_id}), 404
    
    task = background_tasks[task_id]
    task.stop()
    
    return jsonify({
        "status": "task_stopped",
        "task_id": task_id,
        "final_status": task.get_status(),
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/load-test/list', methods=['GET'])
def list_background_tasks():
    tasks = {}
    for task_id, task in background_tasks.items():
        tasks[task_id] = task.get_status()
    
    return jsonify({
        "active_tasks": len([t for t in background_tasks.values() if t.status == "running"]),
        "total_tasks": len(background_tasks),
        "tasks": tasks,
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/load-test/cleanup', methods=['POST'])
def cleanup_completed_tasks():
    global background_tasks
    
    initial_count = len(background_tasks)
    background_tasks = {
        tid: task for tid, task in background_tasks.items() 
        if task.status == "running"
    }
    cleaned_count = initial_count - len(background_tasks)
    
    return jsonify({
        "status": "cleanup_completed",
        "tasks_cleaned": cleaned_count,
        "active_tasks": len(background_tasks),
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/tracking/status', methods=['GET'])
def tracking_status():
    session_info = sequence_tracker.get_session_info()
    current_seq = sequence_tracker.sequence_number
    
    return jsonify({
        "tracking_status": {
            "session_id": session_info['session_id'],
            "instance_id": session_info['instance_id'],
            "start_time": session_info['start_time'],
            "current_sequence": current_seq,
            "uptime_seconds": (datetime.now() - sequence_tracker.start_time).total_seconds(),
            "logs_generated": current_seq
        },
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/tracking/reset', methods=['POST'])
def reset_tracking():
    global sequence_tracker
    old_session = sequence_tracker.session_id
    old_sequence = sequence_tracker.sequence_number
    
    sequence_tracker = SequenceTracker()
    
    return jsonify({
        "status": "tracking_reset",
        "old_session_id": old_session,
        "old_sequence": old_sequence,
        "new_session_id": sequence_tracker.session_id,
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/log-info', methods=['GET'])
def log_info():
    try:
        file_stats = os.stat(log_file_path)
        file_size = file_stats.st_size
        file_modified = datetime.fromtimestamp(file_stats.st_mtime).isoformat()
        
        with open(log_file_path, 'r') as f:
            line_count = sum(1 for _ in f)
    except Exception as e:
        file_size = 0
        file_modified = "never"
        line_count = 0
    
    return jsonify({
        "log_configuration": {
            "stdout_enabled": True,
            "file_enabled": True,
            "log_file_path": log_file_path,
            "log_directory": os.path.dirname(log_file_path)
        },
        "log_file_stats": {
            "size_bytes": file_size,
            "size_mb": round(file_size / 1024 / 1024, 2),
            "line_count": line_count,
            "last_modified": file_modified,
            "exists": os.path.exists(log_file_path)
        },
        "timestamp": datetime.now().isoformat()
    })

@log_generator_bp.route('/stats', methods=['GET'])
def get_stats():
    session_info = sequence_tracker.get_session_info()
    return jsonify({
        "service": "vector-log-generator",
        "version": "1.1.0",
        "namespace": "/log-generator",
        "uptime": (datetime.now() - sequence_tracker.start_time).total_seconds(),
        "logging": {
            "stdout_enabled": True,
            "file_enabled": True,
            "log_file": log_file_path
        },
        "tracking": {
            "session_id": session_info['session_id'],
            "instance_id": session_info['instance_id'],
            "logs_generated": sequence_tracker.sequence_number
        },
        "endpoints": {
            "/log-generator/generate": "Generate single log",
            "/log-generator/generate/bulk": "Generate bulk logs (POST with count, rate_per_second)",
            "/log-generator/load-test/start": "Start background load test (POST with duration, rate)",
            "/log-generator/load-test/status/<task_id>": "Get background task status",
            "/log-generator/load-test/stop/<task_id>": "Stop background task",
            "/log-generator/load-test/list": "List all background tasks",
            "/log-generator/load-test/cleanup": "Clean up completed tasks",
            "/log-generator/health": "Health check",
            "/log-generator/stats": "Service statistics",
            "/log-generator/log-info": "Log file information",
            "/log-generator/tracking/status": "Get tracking status",
            "/log-generator/tracking/reset": "Reset sequence tracking"
        },
        "timestamp": datetime.now().isoformat()
    })

# === ROOT ROUTES (for backwards compatibility) ===

@app.route('/health', methods=['GET'])
def root_health_check():
    return jsonify({"status": "healthy", "namespace": "root", "timestamp": datetime.now().isoformat()})

@app.route('/debug/routes', methods=['GET'])
def debug_routes():
    rules = []
    for rule in app.url_map.iter_rules():
        rules.append({
            "endpoint": rule.endpoint,
            "rule": str(rule),
            "methods": list(rule.methods - {'HEAD', 'OPTIONS'})
        })
    return jsonify({
        "routes": rules, 
        "blueprint_registered": "log_generator" in app.blueprints,
        "blueprints": list(app.blueprints.keys())
    })

# Register the blueprint - THIS MUST HAPPEN AFTER route definitions
app.register_blueprint(log_generator_bp)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'
    
    print(f"Starting Vector Log Generator Service on port {port}")
    print(f"Logging to stdout: ✅")
    print(f"Logging to file: ✅ {log_file_path}")
    print(f"Registered blueprints: {list(app.blueprints.keys())}")
    print("Available routes:")
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            print(f"  {rule}")
    
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)