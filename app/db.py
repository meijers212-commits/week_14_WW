from datetime import datetime
from mysql.connector.connection import MySQLConnectionAbstract
import mysql.connector
from dotenv import load_dotenv
import os


load_dotenv()

db_user = os.getenv("DB_USER")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_password = os.getenv("DB_PASSWORD")

# password=db_password

class Dbinstractot:

    @staticmethod
    def get_connection():
        try:
            connection = mysql.connector.connect(
                host=db_host, user=db_user, password=db_password, database=db_name
            )
            return connection

        except Exception as e:
            raise Exception(e)

    @staticmethod
    def creat_table(connection: MySQLConnectionAbstract):
        try:
            with connection.cursor() as mycursor:
                mycursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS records_weather (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        weapon_id VARCHAR(100), 
                        weapon_name VARCHAR(100),
                        weapon_type VARCHAR(100), 
                        range_km INT,
                        weight_kg FLOAT,
                        manufacturer VARCHAR(100), 
                        origin_country VARCHAR(100), 
                        storage_location VARCHAR(100),
                        year_estimated INT,
                        risk_level VARCHAR(100)
                    )
                    """
                )
                connection.commit()
        except Exception as e:
            raise Exception(f"message:cant create table or table alredy exsisted, Error:{e}")
        
    @staticmethod
    def insert_to_db(data):
        conn = None
        try:
            conn = Dbinstractot.get_connection()
            cursor = conn.cursor(dictionary=True)
            query = """INSERT INTO records_weather(weapon_id, weapon_name, weapon_type,
                    range_km, weight_kg, manufacturer, origin_country,
                    storage_location, year_estimated, risk_level)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for record in data:
                cursor.execute(
                    query,
                    (
                        record["weapon_id"],
                        record["weapon_name"],
                        record["weapon_type"],
                        record["range_km"],
                        record["weight_kg"],
                        record["manufacturer"],
                        record["origin_country"],
                        record["storage_location"],
                        record["year_estimated"],
                        record["risk_level"],
                    ),
                )
            conn.commit()  
            cursor.close()

        except Exception as e:
            if conn:
                conn.rollback() 
            raise Exception(f"message : cant insert data to db successfully, Error:{e}")
        finally:
            if conn:
                conn.close()


