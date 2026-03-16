import mysql.connector
from mysql.connector import errorcode
import os



class MySqlConnection:

    def __init__(self):
        self.host = os.getenv("SQL_HOST")
        self.user = os.getenv("SQL_USER")
        self.password = os.getenv("SQL_PASSWORD")
        self.db = self.create_db()
        self.conn = self.create_connection()


    def create_connection(self):
        try:
            conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password)
            return conn
        except mysql.connector.Error as err:
            raise Exception(f"Connection failed. Details: {err}")
        

    def create_db(self, db_name):
        cursor = self.conn.cursor()
        cursor.execute(f"create database if not exists {db_name}")
        cursor.close()


    def create_target_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            create table if not exists target_table(
            timestamp datetime,
            id_signal varchar(100),
            id_entity varcchar(100) primary key,
            lat_reported float
            lon_reported float
            type_signal char(10)
            priority_level int
            )
            """
            )
        cursor.close()

