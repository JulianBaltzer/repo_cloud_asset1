# Project: Cloud_Assets, Author: Julian Baltzer, Datum: 09.09.2022
from ast import Pass
import shutil
import os
import pandas as pd
import glob
import datetime
import mysql.connector
import sys
import ntpath
import time
from sqlalchemy import true

db = mysql.connector.connect(
    host='localhost',
    database='queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=3306
)


# Die Dateipfade sind auf den server zugeschnitten. Du solltest auch am besten nur noch auf dem Server Testen! 
# Zum ausführen auf dem Server python3 Project/csv_row_check.py eingeben
# Vorher natürlich das script in den Prokect Ordner legen oder ersetzen und vergiss nicht die csv zum testen wieder in dem input zu schieben and öfters die Datenbank leeren 


cursor  = db.cursor()
check = 0
output_information = 0 # Auf 1 setzten, wenn keine Meldungen mehr ausgegeben werden sollen
# Quellverzeichnis
source = "/home/opc/Project/Source/"
# Arbeitsverzeichnis
destination = "/home/opc/Project/arbeitsverzeichnis/"
#Errorverzeichnis
errorverzeichnis = "/home/opc/Project/error/"

done = "/home/opc/Project/Output/"
#Datei in Quellverzeichnis vorhanden?
if output_information == 0:
    print("Checkpoint 1")
while (true):
    if len(os.listdir('/home/opc/Project/Source') ) == 0:
        print("Keine Datei vorhanden")
        time.sleep(1)
        check = 1
    else:    
        allfiles = os.listdir(source)
        check = 0
    if check == 0:
        # Datein in Arbeitsverzeichnis verschieben
        for f in allfiles:
            shutil.move(source+ f, destination + f)
        if output_information == 0:
            print("Checkpoint 2")    
        # Dataframe erstellen
        path  = "/home/opc/Project/arbeitsverzeichnis"
        filenames  = glob.glob(path + "/*.csv.gz")
        
        if output_information == 0:
            print("Checkpoint 3")
            print(filenames)
        for filename in filenames:
            try:
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

                if dataframe.empty:
                            print("Keine Datensätz zu verarbeiten")
                            sys.exit(1)
                
                dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex=r'^tag')))]
                dataframe = dataframe.where((pd.notnull(dataframe)), 0.0)
                dataframe["dbindate"] = pd.to_datetime(dataframe["dbindate"])
                dataframe["dbupdate"] = pd.to_datetime(dataframe["dbupdate"])
                dataframe["lineItem/intervalUsageStart"]= pd.to_datetime(dataframe["lineItem/intervalUsageStart"]) #Hier die umwandlung in für SQL typisches Datetime Format
                dataframe["lineItem/intervalUsageEnd"]= pd.to_datetime(dataframe["lineItem/intervalUsageEnd"])
                rows = dataframe.values.tolist()
                if output_information == 0:
                    print("Checkpoint 5")
                counter = 0
                '''for index, rows in dataframe.iterrows():
                    if output_information == 0:
                        print("Checkpoint 6")
                    try: 
                        query = "INSERT IGNORE INTO queue VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                        cursor.execute(query,(rows["q_status"],
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
                    except (mysql.connector.Error, mysql.connector.Warning) as e:
                        print(e)''' # Dieser Teil funktioniert nicht wegen dem Format der Datetime, versuch das mal zum laufen zu bringen. Alternative siehe unten
                for x in rows:
                    query = "INSERT INTO queue VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(query,(0,rows[counter][14],rows[counter][15],rows[counter][16],rows[counter][17],rows[counter][18],rows[counter][19],rows[counter][0],rows[counter][1],rows[counter][2],rows[counter][3],rows[counter][4],rows[counter][5],rows[counter][6],rows[counter][7],rows[counter][8],rows[counter][9],rows[counter][10],rows[counter][11],rows[counter][12]))
                    counter = counter + 1
                           
                if output_information == 0:
                    print("Checkpoint 6.1")
                tests  = ntpath.basename(filename)
                if output_information == 0:
                    print("Checkpoint 6.2")
                shutil.move(filename, done +tests)  
                if output_information == 0:
                    print("Checkpoint 7")
                db.commit()
            except:
                now = datetime.datetime.now()
                new_name = now.strftime("%Y_%m_%d %H_%M_%S") + " " + ntpath.basename(filename) #Basename nimmt den letzten Teil des Dateipfads
                shutil.move(filename, errorverzeichnis+ new_name)
                pass


    
    
