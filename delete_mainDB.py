import mysql.connector

db = mysql.connector.connect(
    host='localhost',
    database='cloud_assets',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)

cursor = db.cursor()

cursor.execute("DELETE FROM tags_has_costs")
cursor.execute("DELETE FROM tags")
cursor.execute("DELETE FROM costs")
cursor.execute("DELETE FROM resources")
cursor.execute("DELETE FROM sevices")
cursor.execute("DELETE FROM servicestypes")



db.commit()