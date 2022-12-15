import mysql.connector

db2 = mysql.connector.connect(
    host='localhost',
    database='Queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)

cursor = db2.cursor()

cursor.execute("delete from t_to_q")
cursor.execute("delete from tags")
cursor.execute("delete from queue")

db2.commit()