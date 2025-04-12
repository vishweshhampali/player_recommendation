import os
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

class DatabaseController:
    def __init__(self):
        load_dotenv()
        self.connection = None
        self.connect()

    def connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            print("Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def disconnect(self) -> None:
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")

    def create(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """Create a new record in the specified table"""
        try:
            with self.connection.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                query = f"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING id"
                cursor.execute(query, list(data.values()))
                self.connection.commit()
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error creating record: {e}")
            self.connection.rollback()
            return None

    def read(self, table: str, id: int) -> Optional[Dict[str, Any]]:
        """Read a record by ID from the specified table"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"SELECT * FROM {table} WHERE id = %s"
                cursor.execute(query, (id,))
                return cursor.fetchone()
        except Exception as e:
            print(f"Error reading record: {e}")
            return None

    def read_all(self, table: str) -> List[Dict[str, Any]]:
        """Read all records from the specified table"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"SELECT * FROM {table}"
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"Error reading records: {e}")
            return []

    def update(self, table: str, id: int, data: Dict[str, Any]) -> bool:
        """Update a record in the specified table"""
        try:
            with self.connection.cursor() as cursor:
                set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
                query = f"UPDATE {table} SET {set_clause} WHERE id = %s"
                values = list(data.values()) + [id]
                cursor.execute(query, values)
                self.connection.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating record: {e}")
            self.connection.rollback()
            return False

    def delete(self, table: str, id: int) -> bool:
        """Delete a record from the specified table"""
        try:
            with self.connection.cursor() as cursor:
                query = f"DELETE FROM {table} WHERE id = %s"
                cursor.execute(query, (id,))
                self.connection.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting record: {e}")
            self.connection.rollback()
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect() 