import mysql.connector

db = mysql.connector.connect(
    host='localhost',
    database='cloud_assets',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)
db2 = mysql.connector.connect(
    host='localhost',
    database='queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)

cursor = db.cursor()
cursor2 = db2.cursor()

def check_servicetype(servicetype):
    cursor.execute("Insert IGNORE INTO servicestypes (st_type) VALUES ('{}');".format(servicetype))
    db.commit()
    cursor.execute("Select st_id from servicestypes where st_type = '{}'".format(servicetype))
    id = list(cursor.fetchall())
    return id 

def check_services(service, product_sku, currency, billingunit_long, billingunit, service_id):
    print(product_sku,service,currency,billingunit_long,billingunit,service_id[0][0])
    query = "Insert IGNORE INTO sevices(s_productsku, s_name, s_currency, s_billingunit_long, s_billingunit, s_stid) VALUES(%s,%s,%s,%s,%s,%s);"
    cursor.execute(query,(product_sku,service,currency,billingunit_long,billingunit,service_id[0][0]))
    db.commit()
    cursor.execute("Select s_id from sevices where s_name = '{}' AND s_stid = {}".format(service,service_id[0][0]))
    id = list(cursor.fetchall())
    return id 

def check_resources(resource, s_id):
    print(s_id)
    query = "Insert IGNORE INTO resources (r_name, r_sid) VALUES (%s, %s)"
    cursor.execute(query,(resource,s_id[0][0]))
    db.commit()
    query = "Select r_id from resources where r_name = %s AND r_sid = %s"
    cursor.execute(query,(resource,s_id[0][0]))
    id = list(cursor.fetchall())
    print(id)
    return id

'''def check_tags(tags):
    cursor.execute("Select * from tags where t_name = {}".format(tags)) 
    row = cursor.fetchall()
    if row == None:
        cursor.execute("Insert into tags (t_name) VALUES ({})".format(tags))
    cursor.execute("Select t_id from tags where t_name = {}".format(tags))
    id = cursor.fetchall()
    return id   

def fill_tag_to_cost(r_id, tag_id, tag_value):
    cursor.execute("Select c_id from costs where c_rid = {}".format(r_id))
    c_id = cursor.fetchall()
    cursor.execute("Insert into tags_has_cost(tags2c_tid,tags2c_cid,t2c_tagvalue) VALUES ({}, {}, {})".format(tag_id,c_id,tag_value))'''
    
    
cursor2.execute("Select * from queue")
list_of_queue = list(cursor2.fetchall())

cursor2.execute("Select * from queue_status")
list_of_status = list(cursor2.fetchall())

counter = 0
for x in list_of_queue:
    #Alle übergaben an die Funktionen sind nicht korrekt und müssen geändert werden
    startzeit = list_of_queue[counter][7]
    endzeit = list_of_queue[counter][8]
    service = list_of_queue[counter][9]
    compartment = list_of_queue[counter][10]
    resource = list_of_queue[counter][11]
    billedquantity = list_of_queue[counter][12]
    product_sku = list_of_queue[counter][13]
    service_type = list_of_queue[counter][14]
    unit_price = list_of_queue[counter][15]
    mycost = list_of_queue[counter][16]
    currency_code = list_of_queue[counter][17]
    billingunit_long = list_of_queue[counter][18]
    skuUnitDescription = list_of_queue[counter][19]    
    print("Checkpoint 1")
    servicetype_id = check_servicetype(service)
    print("Checkpoint 2")
    service_id = check_services(service_type, product_sku, currency_code, billingunit_long, skuUnitDescription, servicetype_id)
    print("Checkpoint 3")
    res_id  = check_resources(resource,service_id)
    print("Checkpoint 4")
    print(res_id)
    print("über mir")
    if list_of_queue[counter][6] == 0:
        query = "Insert into costs (c_mycost, c_unitprice, c_billedquantity, startzeit, endzeit, compartment_name, c_rid) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(query,(mycost, unit_price, billedquantity, startzeit, endzeit, compartment, res_id[0][0]))
    counter = counter + 1
db2.commit()
