from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

def fetch_recent_data(database_name, table_name, limit=10):
    """Fetch recent data from SQLite database"""
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT ?", (limit,))
    recent_data = cursor.fetchall()
    conn.close()
    return recent_data

@app.route('/recent_data/<table_name>/<int:limit>', methods=['GET'])
def get_recent_data(table_name, limit):
    """API endpoint to get recent data"""
    try:
        recent_data = fetch_recent_data('etl_database.db', table_name, limit)
        return jsonify({'data': recent_data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)