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


cursor  = db.cursor()
check = 0

# Quellverzeichnis
source = "/home/opc/Project/Source/"
# Arbeitsverzeichnis
destination = "/home/opc/Project/arbeitsverzeichnis/"
#Errorverzeichnis
errorverzeichnis = "/home/opc/Project/error/"

done = "/home/opc/Project/Output/"
#Datei in Quellverzeichnis vorhanden?
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
        print("Checkpoint 2")    
        # Dataframe erstellen
        path  = "/home/opc/Project/arbeitsverzeichnis"
        filenames  = glob.glob(path + "/*.csv.gz")
        print("Checkpoint 3")
        print(filenames)
        for filename in filenames:
            try:
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
                dataframe["lineItem/intervalUsageStart"]= pd.to_datetime(dataframe["lineItem/intervalUsageStart"])
                dataframe["lineItem/intervalUsageEnd"]= pd.to_datetime(dataframe["lineItem/intervalUsageEnd"])
                rows = dataframe.values.tolist()
                
                counter = 0
                for x in rows:
                    cursor.execute("INSERT INTO queue VALUES({},'{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}',{},'{}','{}',{},{},'{}','{}','{}');".format(0,rows[counter][14],rows[counter][15],rows[counter][16],rows[counter][17],rows[counter][18],rows[counter][19],rows[counter][0],rows[counter][1],rows[counter][2],rows[counter][3],rows[counter][4],rows[counter][5],rows[counter][6],rows[counter][7],rows[counter][8],rows[counter][9],rows[counter][10],rows[counter][11],rows[counter][12]))
                    counter = counter + 1
                tests  = ntpath.basename(filename)
                shutil.move(filename, done +tests)  

                db.commit()
            except:
                now = datetime.datetime.now()
                new_name = now.strftime("%Y_%m_%d %H_%M_%S") + ntpath.basename(filename)
                shutil.move(filename, errorverzeichnis+ new_name)
                pass


    
    
