import csv
from multiprocessing import connection
import queue
import sqlite3
import shutil
import os
import csv
import pandas as pd
import glob
import datetime
import mysql.connector
import sys



# Quellverzeichnis
source = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/quellverzeichnis/"
# Arbeitsverzeichnis
destination = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/arbeitsverzeichnis/"

#Datei in Quellverzeichnis vorhanden?

if len(os.listdir('C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/quellverzeichnis') ) == 0:
    print("Keine Datei vorhanden")
else:    
    allfiles = os.listdir(source)

# Datein in Arbeitsverzeichnis verschieben
for f in allfiles:
    shutil.move(source+ f, destination + f)
    

# Inhalt in Csv-Datei?
""""path  = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/arbeitsverzeichnis"
csv_files = glob.glob(path + "/*.csv")
df = pd.read_csv.glob.glob(path + "/*.csv")
if df.empty:
    print("Kein Inhalt")
else:
    print(df)"""




# Dataframe erstellen
path  = "C:/Users/Michael.Malkmus/OneDrive - FUNKE Mediengruppe/Desktop/Cloud Assets Projekt/csv_import/arbeitsverzeichnis"
filenames  = glob.glob(path + "/*.csv")



for filename in filenames:
    try:
        dataframe = pd.read_csv(filename, sep=',',low_memory=False)
        rows = dataframe.values.tolist()
        if dataframe.empty:
            print("Keine Datensätz zu verarbeiten")
            sys.exit(1)
        
        for x in rows:
            dataframe["queue_status"] = 0
            now = datetime.datetime.now()
            dataframe["dbindate"] = now.strftime("%d/%m/%Y %H:%M:%S")
            dataframe["dbinuser"] = ""
            dataframe["dbupdateuser"] = ""
            dataframe["queue_status"] = ""
            dataframe["dbupdate"] = now.strftime("%d/%m/%Y %H:%M:%S")   
            
            tags_df = dataframe.filter(regex=r'^tag')
            #tags_df.to_sql()#Für tags in Sql Transformation
            
            # Alle nicht benötigten Spalten rausschmeißen
            
        #dataframe.drop(['lineItem/isCorrection'], axis='columns')
            
    except:
        print("Error in" + filename)
        
dataframe


dataframe
# Verbindung Datenbank

queue = mysql.connector.connect(
host="10.102.137.197",
user="root", 
port=22,
database="cloud_assets"                                 
)

print(queue)


import mysql.connector

cnx = mysql.connector.connect(
    host='10.102.137.197',
    database='cloud_assets',
    user='root',
    password='Gu6dQVQbMXFsPQQ7!',
    port=22
)
    
    
    

"""test = df.iloc[0:1]
print(test)
for row in df:
    if df[row] == 0:
        print("zeile leer")
    else:
        print("toll")
        #inhalt ist da
        
print("test")"""

#
