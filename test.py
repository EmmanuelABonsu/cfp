import unittest
import os
import sqlite3

from etl_script import create_database, create_table, ingest_csv, etl_pipeline

class TestETLScript(unittest.TestCase):
    def setUp(self):
        self.database_name = "test_database.db"
        self.csv_folder_path = "test_csv_files"
        self.create_test_csv_files()
        self.create_database()

    def tearDown(self):
        os.remove(self.database_name)
        for file_name in os.listdir(self.csv_folder_path):
            os.remove(os.path.join(self.csv_folder_path, file_name))
        os.rmdir(self.csv_folder_path)

    def create_test_csv_files(self):
        os.mkdir(self.csv_folder_path)
        # Create test CSV files
        with open(os.path.join(self.csv_folder_path, "test_table1.csv"), "w") as file:
            file.write("id,name\n1,Alice\n2,Bob\n")
        with open(os.path.join(self.csv_folder_path, "test_table2.csv"), "w") as file:
            file.write("id,age\n1,30\n2,25\n")

    def create_database(self):
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test_table1 (id INT, name TEXT)")
        cursor.execute("CREATE TABLE test_table2 (id INT, age INT)")
        conn.commit()
        conn.close()

    def test_create_database(self):
        self.assertTrue(os.path.exists(self.database_name))

    def test_create_table(self):
        conn = sqlite3.connect(self.database_name)
        create_table(conn, "test_table3", "id INT, name TEXT")
        conn.close()
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(test_table3)")
        columns = cursor.fetchall()
        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[0][1], "id")
        self.assertEqual(columns[1][1], "name")
        conn.close()

    def test_ingest_csv(self):
        conn = sqlite3.connect(self.database_name)
        ingest_csv(conn, os.path.join(self.csv_folder_path, "test_table1.csv"))
        ingest_csv(conn, os.path.join(self.csv_folder_path, "test_table2.csv"))
        conn.close()
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table1")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
        cursor.execute("SELECT COUNT(*) FROM test_table2")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
        conn.close()

    def test_etl_pipeline(self):
        etl_pipeline(self.database_name, self.csv_folder_path)
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table1")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
        cursor.execute("SELECT COUNT(*) FROM test_table2")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
        conn.close()

if __name__ == "__main__":
    unittest.main()