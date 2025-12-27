from flask import Flask, jsonify, request
import os
import json
from datetime import datetime

app = Flask(__name__)

# Configuration from environment variables
DB_NAME = os.getenv('DB_NAME', 'demo_db')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')
MAX_CONNECTIONS = int(os.getenv('MAX_CONNECTIONS', '100'))

# In-memory database simulation
database = {
    "users": {
        1: {"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2024-01-01"},
        2: {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": "2024-01-02"},
        3: {"id": 3, "name": "Bob Wilson", "email": "bob@example.com", "created_at": "2024-01-03"}
    },
    "items": {
        1: {"id": 1, "name": "Product A", "price": 29.99, "stock": 100},
        2: {"id": 2, "name": "Product B", "price": 49.99, "stock": 50},
        3: {"id": 3, "name": "Product C", "price": 19.99, "stock": 200}
    },
    "orders": {
        1: {"id": 1, "user_id": 1, "item_id": 1, "quantity": 2, "status": "completed"},
        2: {"id": 2, "user_id": 2, "item_id": 2, "quantity": 1, "status": "pending"}
    }
}

# Connection tracking
connections = {
    "active": 0,
    "total": 0,
    "max": MAX_CONNECTIONS
}

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "demo-database",
        "database": DB_NAME,
        "environment": ENVIRONMENT
    })

@app.route('/ready')
def ready():
    """Readiness check endpoint"""
    return jsonify({
        "status": "ready",
        "service": "demo-database",
        "connections": connections
    })

@app.route('/stats')
def stats():
    """Database statistics"""
    return jsonify({
        "database": DB_NAME,
        "environment": ENVIRONMENT,
        "tables": list(database.keys()),
        "record_counts": {table: len(records) for table, records in database.items()},
        "connections": connections,
        "uptime": "running"
    })

@app.route('/query', methods=['POST'])
def query():
    """Execute a simulated database query"""
    data = request.get_json() or {}
    operation = data.get('operation', 'select')
    table = data.get('table', '')
    conditions = data.get('conditions', {})
    values = data.get('values', {})
    
    if table not in database:
        return jsonify({"error": f"Table '{table}' not found"}), 404
    
    connections['active'] += 1
    connections['total'] += 1
    
    try:
        if operation == 'select':
            results = list(database[table].values())
            if conditions:
                results = [r for r in results if all(r.get(k) == v for k, v in conditions.items())]
            return jsonify({
                "operation": "select",
                "table": table,
                "results": results,
                "count": len(results)
            })
        
        elif operation == 'insert':
            new_id = max(database[table].keys()) + 1 if database[table] else 1
            values['id'] = new_id
            database[table][new_id] = values
            return jsonify({
                "operation": "insert",
                "table": table,
                "inserted_id": new_id,
                "record": values
            }), 201
        
        elif operation == 'update':
            record_id = conditions.get('id')
            if record_id and record_id in database[table]:
                database[table][record_id].update(values)
                return jsonify({
                    "operation": "update",
                    "table": table,
                    "updated_id": record_id,
                    "record": database[table][record_id]
                })
            return jsonify({"error": "Record not found"}), 404
        
        elif operation == 'delete':
            record_id = conditions.get('id')
            if record_id and record_id in database[table]:
                deleted = database[table].pop(record_id)
                return jsonify({
                    "operation": "delete",
                    "table": table,
                    "deleted_id": record_id,
                    "record": deleted
                })
            return jsonify({"error": "Record not found"}), 404
        
        else:
            return jsonify({"error": f"Unknown operation: {operation}"}), 400
    
    finally:
        connections['active'] -= 1

@app.route('/tables')
def list_tables():
    """List all tables in the database"""
    return jsonify({
        "tables": [
            {
                "name": table,
                "record_count": len(records),
                "columns": list(list(records.values())[0].keys()) if records else []
            }
            for table, records in database.items()
        ]
    })

@app.route('/tables/<table_name>')
def get_table(table_name):
    """Get all records from a specific table"""
    if table_name not in database:
        return jsonify({"error": f"Table '{table_name}' not found"}), 404
    
    return jsonify({
        "table": table_name,
        "records": list(database[table_name].values()),
        "count": len(database[table_name])
    })

@app.route('/tables/<table_name>/<int:record_id>')
def get_record(table_name, record_id):
    """Get a specific record from a table"""
    if table_name not in database:
        return jsonify({"error": f"Table '{table_name}' not found"}), 404
    
    if record_id not in database[table_name]:
        return jsonify({"error": f"Record {record_id} not found in '{table_name}'"}), 404
    
    return jsonify(database[table_name][record_id])

@app.route('/backup', methods=['POST'])
def backup():
    """Simulate database backup"""
    timestamp = datetime.now().isoformat()
    return jsonify({
        "status": "backup_completed",
        "timestamp": timestamp,
        "tables_backed_up": list(database.keys()),
        "total_records": sum(len(records) for records in database.values())
    })

@app.route('/write')
def write():
    """Legacy endpoint for compatibility"""
    return jsonify({"msg": "Data written to Demo-Database"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
