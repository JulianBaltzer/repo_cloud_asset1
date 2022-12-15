# Project: Cloud_Assets, Author: Julian Baltzer, Michael Malkmus Datum: 14.12.2022
# Version: 0.2.2.1 major_update/minor_update/patch/hotfix
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
    database='Queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)

cursor = db.cursor()
cursor2 = db2.cursor()

def check_servicetype(servicetype):
   # print(servicetype)
    cursor.execute("Select st_id from servicestypes where st_type = '{}'".format(servicetype))
    id = cursor.fetchone()
   # print(id)
    if not id:
        query  = "Select max(st_id) from servicestypes"
        cursor.execute(query)
        max  = list(cursor.fetchone())
        if max[0] == None:
           # print("if not max")
            max[0] = 0
        max[0] = max[0] + 1
       # print("test")
        cursor.execute("Insert INTO servicestypes (st_id,st_type) VALUES ('{}','{}');".format(max[0],servicetype))
        db.commit()
        return max
    
    return id 

def check_services(service, product_sku, currency, billingunit_long, billingunit, service_id):
    #print(service_id)
    cursor.execute("Select s_id from sevices where s_name = '{}' AND s_stid = '{}'".format(service,service_id))
    id = cursor.fetchone()
    if id == None:
        query  = "Select max(s_id) from sevices"
        cursor.execute(query)
        max  = list(cursor.fetchone())
        if max[0] == None:
            #print("if not max")
            max[0] = 0
        max[0] = max[0] + 1
        cursor.execute("Insert INTO sevices(s_id, s_productsku, s_name, s_currency, s_billingunit_long, s_billingunit, s_stid) VALUES('{}','{}','{}','{}','{}','{}','{}');".format(max[0],product_sku,service,currency,billingunit_long,billingunit,service_id))
        id = cursor.fetchone()
        db.commit()
        return max
    return id 

counter = 0

def check_resources(resource, s_id): 
    cursor.execute("Select r_id from resources where r_name = '{}'".format(resource,s_id))
    id = cursor.fetchone()
    if id == None:
       # print(type(s_id))
        query  = "Select max(r_id) from resources"
        cursor.execute(query)
        max  = list(cursor.fetchone())
        if max[0] == None:
            #print("if not max")
            max[0] = 0
        max[0] = max[0] + 1
        cursor.execute("Insert INTO resources (r_id, r_name, r_sid) VALUES ('{}', '{}', '{}')".format(max[0],resource,s_id))
        id = cursor.fetchone()
        db.commit()
        return max
    return id

def fill_tag_to_cost(q_id, c_id):
    print(q_id)
    query  = "Select tag_ID, tag_value from t_to_q where q_id = '{}'".format(q_id)
    cursor2.execute(query)
    result  = list(cursor2.fetchall())
    print(result)
    #print(max)
    
    query = "Select max(tags2c_id) from tags_has_costs"
    cursor.execute(query)
    max  = list(cursor.fetchone())
    if max[0] == None:
            max[0] = 1
    for x in result:
        max[0] = max[0] + 1
        cursor.execute("Insert into tags_has_costs(tags2c_id,tags2c_tid,tags2c_cid,t2c_tagvalue) VALUES ('{}', '{}', '{}', '{}')".format(max[0],x[0],c_id,x[1]))
        
def get_set_tags(tags_data):
      
    tags_data = str(tags_data)
    query2 = "Select t_id from tags where t_name = (%s)"
    cursor.execute(query2, (tags_data,), False)
    
    row = cursor.fetchone() # Für die überprüfung ob row = null muss nicht in list convertiert werden!
    if not row:
        query1  = "Select max(t_id) from tags"
        cursor.execute(query1)

        max  = list(cursor.fetchone())  # Return aus cursor.fetchone() ist immer tuple. Wertzuweisung ist bei tuple nicht möglich deswegen list()

        if max[0] == None:
            max[0] = 0
        max[0] = max[0] + 1

        cursor.execute("Insert into tags(t_id,t_name) VALUES ('{}','{}')".format(max[0],tags_data))
        db.commit()
        

cursor2.execute("Select * from queue where queue_status_qs_id = 0 LIMIT 5000")
list_of_queue = list(cursor2.fetchall())

cursor2.execute("Select * from queue_status")
list_of_status = list(cursor2.fetchall())

cursor2.execute("Select * from tags")
tags = list(cursor2.fetchall())

for x in tags:
    get_set_tags(x[1])

for x in list_of_queue:
    print(counter)
    q_id = x[0]
    print(q_id)
    startzeit = x[7]
    endzeit = x[8]
    service = x[9]
    compartment = x[10]
    resource = x[11]
    billedquantity = x[12]
    product_sku = x[13]
    service_type = x[14]
    unit_price = x[15]
    mycost = x[16]
    currency_code = x[17]
    billingunit_long = x[18]
    skuUnitDescription = x[19]    
    #print("Checkpoint 1")
    servicetype_id = check_servicetype(service)
    #print("Checkpoint 2")
   # print(servicetype_id)
    service_id = check_services(service_type, product_sku, currency_code, billingunit_long, skuUnitDescription, servicetype_id[0])
    #print("Checkpoint 3")
    res_id  = check_resources(resource,service_id[0])
    #print("Checkpoint 4")
    #print(res_id)
    #print("über mir")
    
    query  = "Select max(c_id) from costs"
    cursor.execute(query)
    max  = list(cursor.fetchone())
    if max[0] == None:
        print("if not max")
        max[0] = 0
    max[0] = max[0] + 1
    
   # print(billedquantity)
    cursor.execute("Insert INTO costs (c_id, c_mycost, c_unitprice, c_billedquantity, startzeit, endzeit, compartment_name, c_rid) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(max[0], mycost, unit_price, billedquantity, startzeit, endzeit, compartment, res_id[0]))
    fill_tag_to_cost(q_id, max[0])
    counter = counter + 1
    #test
query = "UPDATE queue SET queue_status_qs_id = 1 WHERE queue_status_qs_id = 0 LIMIT 5000"
cursor2.execute(query)
    
db2.commit()
