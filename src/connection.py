import os
import mysql.connector as mysql

config = mysql.connect(
            host = os.getenv("MYSQL_HOST"),
            user = os.getenv("MYSQL_USER"),
            passwd = os.getenv("MYSQL_PASS"),
            database = os.getenv("MYSQL_DB")
        )

def execute_sql(sql):
    try:
        db = config
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        print(e)

def get_ncs(sql):
    try:
        db = config
        cursor = db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        print(e)
