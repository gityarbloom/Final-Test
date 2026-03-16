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
            create table if not exists target(
            timestamp datetime,
            signal_id varchar(100) primary key,
            entity_id varcchar(100),
            reported_lat float
            reported_lon float
            type_signal char(10)
            priority_level int
            speed int
            )
            """
            )
        cursor.close()


    def create_attack_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            create table if not exists attack(
            timestamp datetime,
            attack_id varchar(100) primary key,
            entity_id varcchar(100),
            weapon_type varcchar(100)
            speed int
            )
            """
            )
        cursor.close()


    def create_damage_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            create table if not exists damage(
            timestamp datetime,
            attack_id varchar(100),
            entity_id varcchar(100),
            result varcchar(100)
            speed int
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
            where entity_id = {entity_id}
            and
            result = destroyed
            limit 1
            )
            """
            )
        destroyed = cursor.fetchall()
        cursor.close()
        return destroyed
    

    def is_exists_in_target_banck(self, entity_id):
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            select *
            from target
            where entity_id = {entity_id}
            )
            """
            )
        exists = cursor.fetchall()
        cursor.close()
        return exists
    

    def insert_a_new_target(self, values: list):
        cursor = self.conn.cursor()
        query = """
            insert into target
                (timestamp, signal_id, entity_id, reported_lat, reported_lon, signal_type, priority_level, speed)
                values (%s, %s, %s, %s, %s, %s, %s, %s))
            """
        data = (value for value in values)
        cursor.execute(query, data)
        self.conn.commit()
        cursor.close()

    def get_lat_lon(entity_id):
        cursor = self.conn.cursor()
        cursor.execute(
       
            )
        exists = cursor.fetchall()
        cursor.close()