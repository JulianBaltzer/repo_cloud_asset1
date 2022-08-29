from cgi import test
from hashlib import new
from multiprocessing import connection
import shutil
import os
import pandas as pd
import glob
import datetime
import mysql.connector
import sys

db = mysql.connector.connect(
    host='localhost',
    database='queue',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=22
)

cursor  = db.cursor()

# Quellverzeichnis
source = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/quellverzeichnis/"
# Arbeitsverzeichnis
destination = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/arbeitsverzeichnis/"
#Errorverzeichnis
errorverzeichnis = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/errorverzeichnis/"

done = "Pfad hier einfügen"
#Datei in Quellverzeichnis vorhanden?

if len(os.listdir('C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/quellverzeichnis') ) == 0:
    print("Keine Datei vorhanden")
else:    
    allfiles = os.listdir(source)

# Datein in Arbeitsverzeichnis verschieben
for f in allfiles:
    shutil.move(source+ f, destination + f)
    
# Dataframe erstellen
path  = "C:/Users/jbalt/Documents"
filenames  = glob.glob(path + "\*.csv")

for filename in filenames:
    try:
        dataframe = pd.read_csv(filename, sep=',',low_memory=False)
        dataframe["q_status"] = 0
        now = datetime.datetime.now()
        dataframe["dbindate"] = now.strftime("%d/%m/%Y %H:%M:%S")
        dataframe["dbinuser"] = ""
        dataframe["dbupdateuser"] = ""
        dataframe["q_message"] = ""
        dataframe["dbupdate"] = now.strftime("%d/%m/%Y %H:%M:%S")
        dataframe["queue_status"] = "0"

        dataframe.drop(['lineItem/referenceNo','lineItem/tenantId', 'product/compartmentId', 'product/region', 'product/availabilityDomain',
                        'usage/billedQuantityOverage', 'cost/subscriptionId', 'cost/unitPriceOverage', 'cost/myCostOverage',
                        'cost/overageFlag', 'lineItem/isCorrection', 'lineItem/backreferenceNo'], axis='columns', inplace=True)
        
        tags_df = dataframe.filter(regex=r'^tag')

        if dataframe.empty:
                    print("Keine Datensätz zu verarbeiten")
                    sys.exit(1)

        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex=r'^tag')))]
        rows = dataframe.values.tolist()

        

        cursor.execute("Insert into q_status VALUES(0,'Nicht verarbeitet')")
        cursor.execute("Insert into q_status VALUES(1,'Verarbeitet')")

        for x in rows:
            cursor.execute("Insert into q VALUES({dbindate},{dbinuser},{dbupdateuser},{message},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})".format(rows[x][13],rows[x][14],rows[x][15],rows[x][16],rows[x][17],rows[x][18],rows[x][19],rows[x][0],rows[x][1],rows[x][2],rows[x][3],rows[x][4],rows[x][5],rows[x][6],rows[x][7],rows[x][8],rows[x][9],rows[x][10],rows[x][11],rows[x][12]))
            
        shutil.move(destination+ filename, done + filename)  
    except:
        print("Error in" + filename)
        # Errorverzeichnis verschieben 
        now = datetime.datetime.now()
        new_name = now.strftime("%d_%m_%Y %H_%M_%S") + filename
        os.rename(destination+filename, destination+ new_name)
        shutil.move(destination+ new_name, errorverzeichnis+ new_name)

    
    #TO DOS 
# Tags tabelle insert 
# praxitest






    
    
