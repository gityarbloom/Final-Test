import mysql.connector
import os



class MySqlTargetDB:

    def __init__(self):
        self.host = os.getenv("SQL_HOST")
        self.user = os.getenv("SQL_USER")
        self.password = os.getenv("SQL_PASSWORD")
        self.conn = self.create_connection()
        self.db = self.create_db()
        self.target = self.create_target_table()
        self.attac = self.create_attac_table()
        self.damage = self.create_damage_table()


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
            signal_id varchar(100) primary key,
            entity_id varcchar(100),
            lat_reported float
            lon_reported float
            type_signal char(10)
            priority_level int
            )
            """
            )
        cursor.close()


    def create_attac_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            create table if not exists target_table(
            timestamp datetime,
            attack_id varchar(100) primary key,
            entity_id varcchar(100),
            type_weapon varcchar(100)
            )
            """
            )
        cursor.close()


    def create_damage_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            create table if not exists target_table(
            timestamp datetime,
            attack_id varchar(100),
            entity_id varcchar(100),
            result varcchar(100)
            )
            """
            )
        cursor.close()


    def is_destroyed(self, entity_id):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            select *
            from damage
            where attack_id = {entity_id}
            and
            result = destroyed
            limit 1
            )
            """
            )
        destroyed = cursor.fetchall()
        cursor.close()
        return destroyed