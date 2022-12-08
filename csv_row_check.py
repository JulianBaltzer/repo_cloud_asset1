# Project: Cloud_Assets, Author: Julian Baltzer, Datum: 02.12.2022
# Version: 0.2.2.0 major_update/minor_update/patch/hotfix
from ast import In, Pass
import shutil
import os
import pandas as pd
import glob
import datetime
import mysql.connector
import sys
import ntpath
import time

from sqlalchemy import create_engine
from uuid import uuid4


db = mysql.connector.connect(
    host='localhost',
    database='Queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)

mycursor = db.cursor()

def get_set_tags(tags_data):

    tags_data = str(tags_data)
    query2 = "Select tag_ID from tags where tag_name = (%s)"
    mycursor.execute(query2, (tags_data,), False)
    
    row = cursor.fetchone()
    if not row:
        query1  = "Select max(tag_ID) from tags"
        mycursor.execute(query1)
        print("1")
        max  = list(cursor.fetchone())  # Return aus cursor.fetchone() ist tuple. Wertzuweisung ist bei tuple nicht möglich deswegen list()
        print("2")
        if max[0] == None:
            max[0] = 0
        max[0] = max[0] + 1
        print("3")
        cursor.execute("Insert into tags(tag_ID,tag_name) VALUES ({},'{}')".format(max[0],tags_data))
        db.commit()
        return max
    return list(row) # Hier wird row zur list gemacht, da der aktuelle type tuple ist und dieser nicht änderbar ist. Wäre ich in z.38 möglich


def fill_tag_to_asset(q_id, tag_id, tag_value):
    query1  = "Select max(t_ID) from t_to_q"
    mycursor.execute(query1)
    max  = list(cursor.fetchone())
    
    if max[0] == None:
            max[0] = 0
    max[0] = max[0] + 1
    cursor.execute("Insert into t_to_q(t_ID,tag_ID,q_id,tag_value) VALUES ({}, {}, '{}', '{}')".format(max[0],tag_id,q_id,tag_value))
    
cursor  = db.cursor()
check = 0
output_information = 1 

source = "/home/opc/Project/Source/"
destination = "/home/opc/Project/arbeitsverzeichnis/"
errorverzeichnis = "/home/opc/Project/error/"
done = "/home/opc/Project/Output/"

if output_information == 0:
    print("Checkpoint 1")

if len(os.listdir('/home/opc/Project/Source') ) == 0:
    print("Keine Datei vorhanden")
    time.sleep(1)
    check = 1
else:    
    allfiles = os.listdir(source)
    check = 0
if check == 0:
    for f in allfiles:
        shutil.move(source+ f, destination + f)
        
    if output_information == 0:
        print("Checkpoint 2")    
        
    path  = "/home/opc/Project/arbeitsverzeichnis"
    filenames  = glob.glob(path + "/*.csv.gz")
    
    if output_information == 0:
        print("Checkpoint 3")
        print(filenames)
        
    for filename in filenames:
        try:
            counter = 0
            counter1 = 0
            start = datetime.datetime.now()
            if output_information == 0:
                print("Checkpoint 4")
            dataframe = pd.read_csv(filename, sep=',',low_memory=False,compression='gzip')
            dataframe["q_status"] = 0
            now = datetime.datetime.now()
            dataframe["dbindate"] = now.strftime("%Y-%m-%d %H:%M:%S")
            dataframe["dbinuser"] = "jbaltzer"
            dataframe["dbupdateuser"] = "jbaltzer"
            dataframe["q_message"] = "tests"
            dataframe["dbupdate"] = now.strftime("%Y-%m-%d %H:%M:%S")
            dataframe["queue_status"] = "0"
            
            dataframe.drop(['lineItem/referenceNo','lineItem/tenantId', 'product/compartmentId', 'product/region', 'product/availabilityDomain',
                            'usage/billedQuantityOverage', 'cost/subscriptionId', 'cost/unitPriceOverage', 'cost/myCostOverage',
                            'cost/overageFlag', 'lineItem/isCorrection', 'lineItem/backreferenceNo'], axis='columns', inplace=True)
            tags_df = dataframe.filter(regex=r'^tag')

            print("Checkpoint 4.1")
            print(tags_df)
            print("Checkpoint 4.2")
            print(tags_df.iloc[[0][0]])
            print("Checkpoint 4.3")
            print(dataframe)     
            print("Checkpoint 4.4")
            
            if dataframe.empty:
                        print("Keine Datensätze zu verarbeiten")
                        break 
            
            dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex=r'^tag')))]
            dataframe = dataframe.where((pd.notnull(dataframe)), 0.0)
            rows = dataframe.values.tolist()
            if output_information == 0:
                print("Checkpoint 5")
            list_of_q_ids = []
            counter = 0
            percent  = dataframe.shape[0]
            percent_counter = 1
            
            for index, rows in dataframe.iterrows():
                print(str(percent_counter) + "/" + str(percent))
                percent_counter += 1
                try: 
                    
                    q_id = str(uuid4())
                    query = "INSERT IGNORE INTO queue VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    cursor.execute(query,(q_id,
                                        rows["dbindate"],
                                        rows["dbinuser"],
                                        rows["dbupdateuser"],
                                        rows["q_message"],
                                        rows["dbupdate"],
                                        rows["queue_status"],
                                        rows["lineItem/intervalUsageStart"],
                                        rows["lineItem/intervalUsageEnd"],
                                        rows["product/service"],
                                        rows["product/compartmentName"],
                                        rows["product/resourceId"],
                                        rows["usage/billedQuantity"],
                                        rows["cost/productSku"],
                                        rows["product/Description"],
                                        rows["cost/unitPrice"],
                                        rows["cost/myCost"],
                                        rows["cost/currencyCode"],
                                        rows["cost/billingUnitReadable"],
                                        rows["cost/skuUnitDescription"]))
                    db.commit()
                    
                    for column in tags_df.columns:
                        #if counter == 0:
                            #print("0.1")
                        id = get_set_tags(column)
                            
                        #Startzeit: 2022-12-01 08:42:01.652618 Endzeit: 2022-12-01 08:45:34.893419
                        #Mit Filter
                        #Startzeit: 2022-12-01 08:47:41.714866 Endzeit: 2022-12-01 09:06:53.929789
                        #Ohne Filter
                        # counter = 0
                        # for values in range(0,len(list_of_q_ids)):
                        #print(str(values) +  str(column) + str(tags_df.loc[values,column]))
                        
                        """Funktioniert nicht richtig. ID bleibt bei 22"""
                        if len(str(tags_df.loc[counter,column])) > 3:
                                fill_tag_to_asset(q_id,id[0],tags_df.loc[counter,column])  
                        
                              
                        #fill_tag_to_asset(list_of_q_ids[counter],id[0],tags_df.loc[values,column])
                        #counter += 1
                        #list_of_q_ids.append(str(q_id)) 
                    counter += 1
                    
                             
                except:
                    raise
   
            if output_information == 0:
                print("Checkpoint 6.1")
            tests  = ntpath.basename(filename)
            if output_information == 0:
                print("Checkpoint 6.2")
            now = datetime.datetime.now()
            shutil.move(filename,done+now.strftime("%Y_%m_%d %H_%M_%S")+tests)  
            if output_information == 0:
                print("Checkpoint 7")
            db.commit()
            end = datetime.datetime.now()
            print("Startzeit:"+str(start)+"Endzeit: "+str(end))
        except Exception as e:
            print(repr(e))
            now = datetime.datetime.now()
            shutil.move(filename,errorverzeichnis+now.strftime("%Y_%m_%d %H_%M_%S")+ntpath.basename(filename))
            error_file_name = ntpath.basename(filename) + "_" + "error.txt"
            with open(errorverzeichnis+error_file_name ,"w") as f:
             f.write("Error in" + filename + "\n" +str(e) + "\n" +str(now.strftime("%Y_%m_%d %H_%M_%S")))
             f.close()
            pass
            

