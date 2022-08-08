import mysql.connector

db = mysql.connector.connect(
    host = "",
    user = "",
    passwd = "",
    database = ""
)
db2 = mysql.connector.connect(
    host = "",
    user = "",
    passwd = "",
    database = ""
)

cursor = db.cursor()
cursor2 = db.cursor()

def check_servicetype(servicetype):
    cursor.execute("Select * from servicetypes where st_type  = {servicetype}".format(servicetype))
    row = cursor.fetchall()
    if row == None:
        cursor.execute("Insert into servicetypes (st_type) VALUES ({servicetype})".format(servicetype))
    cursor.execute("Select st_id from servicetypes where st_type = {servicetype}".format(servicetype))
    id = cursor.fetchall()
    return id 

def check_services(service, product_sku, currency, billingunit_long, billingunit, service_id):
    cursor.execute("Select * from services where s_name = {service} AND s_stid = {service_id}".format(service,service_id))
    row = cursor.fetchall()
    if row == None:
        cursor.execute("Insert into services (s_productsku, s_name, s_currency, s_billingunit_long, s_billingunit, s_stid) VALUES({product_sku},{service},{currency},{billingunit_long},{billingunit},{service_id})".format(product_sku,service,currency,billingunit_long,billingunit,service_id))
    cursor.execute("Select s_id from services where s_name = {service} AND s_stid = {service_id}".format(service,service_id))
    id = cursor.fetchall()
    return id 

def check_resources(resource, s_id):
    cursor.execute("Select * from resources where r_name = {resource} AND r_sid = {s_id}".format(resource,s_id))
    row = cursor.fetchall()
    if row == None:
        cursor.execute("Inser into resources (r_name, r_sid) VALUES ({resource}, {s_id})".format(resource,s_id))
    cursor.execute("Select r_id from resources where r_name = {resource} AND r_sid = {s_id}".format(resource,s_id))
    id = cursor.fetchall()
    return id

def check_tags(tags):
    cursor.execute("Select * from tags where t_name = {tags}".format(tags)) 
    row = cursor.fetchall()
    if row == None:
        cursor.execute("Insert into tags (t_name) VALUES ({tags})".format(tags))
    cursor.execute("Select t_id from tags where t_name = {tags}".format(tags))
    id = cursor.fetchall()
    return id   

def fill_tag_to_cost(r_id, tag_id, tag_value):
    cursor.execute("Select c_id from costs where c_rid = {r_id}".format(r_id))
    c_id = cursor.fetchall()
    cursor.execute("Insert into tags_has_cost(tags2c_tid,tags2c_cid,t2c_tagvalue) VALUES ({tag_id}, {c_id}, {tag_value})".format(tag_id,c_id,tag_value))
    
    
cursor.execute("Select * from q")
list_of_queue = list(cursor.fetchall())

cursor.execute("Select * from queue_status")
list_of_status = list(cursor.fetchall())

for x in list_of_queue:
    #Alle übergaben an die Funktionen sind nicht korrekt und müssen geändert werden
    startzeit = list_of_queue[x][0]
    endzeit = list_of_queue[x][1]
    service = list_of_queue[x][2]
    compartment = list_of_queue[x][3]
    resource = list_of_queue[x][4]
    billedquantity = list_of_queue[x][5]
    product_sku = list_of_queue[x][6]
    service_type = list_of_queue[x][7]
    unit_price = list_of_queue[x][8]
    mycost = list_of_queue[x][9]
    currency_code = list_of_queue[x][10]
    billingunit_long = list_of_queue[x][11]
    skuUnitDescription = list_of_queue[x][12]    
    
    servicetype_id = check_servicetype(service)
    service_id = check_services(service_type, product_sku, currency_code, billingunit_long, skuUnitDescription, servicetype_id)
    res_id  = check_resources(resource,service_id)
    
    cursor.execute("Insert into costs (c_mycost, c_unitprice, c_billedquantity, startzeit, endzeit, compartment_name, c_rid) VALUES ({mycost},{unit_price},{billedquantity},{startzeit},{endzeit},{compartment},{res_id})".format(
                   mycost, unit_price, billedquantity, startzeit, endzeit, compartment, res_id))
    
    test = 1+2
    hallo = "dfgidjfgjidfjigji"