import os
import csv
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='etl_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_database(db_name):
    """Create SQLite database"""
    try:
        conn = sqlite3.connect(db_name)
        logging.info("Database created successfully")
        return conn
    except sqlite3.Error as e:
        logging.error("Error creating database: %s" % e)
        raise

def create_table(conn, table_name, columns):
    """Create table in SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        conn.commit()
        logging.info(f"Table '{table_name}' created successfully")
    except sqlite3.Error as e:
        logging.error(f"Error creating table '{table_name}': {e}")

def ingest_csv(conn, file_path):
    """Ingest data from CSV file into database"""
    try:
        table_name = os.path.basename(file_path).replace('.csv', '')
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            columns = next(reader)  # Extract column headers
            create_table(conn, table_name, ','.join(columns))
            cursor = conn.cursor()
            for row in reader:
                cursor.execute(f"INSERT INTO {table_name} VALUES ({','.join(['?'] * len(row))})", row)
            conn.commit()
            logging.info(f"Data from {file_path} ingested into table '{table_name}' successfully")
    except Exception as e:
        logging.error(f"Error ingesting data from {file_path}: {e}")

def etl_pipeline(db_name, csv_folder):
    """ETL pipeline to ingest CSV files into SQLite database"""
    try:
        conn = create_database(db_name)
        for file_name in os.listdir(csv_folder):
            if file_name.endswith('.csv'):
                file_path = os.path.join(csv_folder, file_name)
                ingest_csv(conn, file_path)
        logging.info("ETL pipeline completed successfully")
        conn.close()
    except Exception as e:
        logging.error(f"Error in ETL pipeline: {e}")

if __name__ == "__main__":
    database_name = "etl_database.db"
    csv_folder_path = "csv_files"
    etl_pipeline(database_name, csv_folder_path)