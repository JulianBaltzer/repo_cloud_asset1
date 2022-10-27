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
from sqlalchemy import create_engine

check = 0
output_information = 0 # Auf 1 setzten, wenn keine Meldungen mehr ausgegeben werden sollen

source = "C:/Users/Julian.Baltzer/OneDrive - FUNKE Mediengruppe/Desktop/tests/"
destination = "/home/opc/Project/arbeitsverzeichnis/"
errorverzeichnis = "/home/opc/Project/error/"
done = "/home/opc/Project/Output/"


if output_information == 0:
    print("Checkpoint 1")


if len(os.listdir('C:/Users/Julian.Baltzer/OneDrive - FUNKE Mediengruppe/Desktop/tests/') ) == 0:
    print("Keine Datei vorhanden")
    time.sleep(1)
    check = 1
else:    
    allfiles = os.listdir(source)
    check = 0
if check == 0:

    path  = "C:/Users/Julian.Baltzer/OneDrive - FUNKE Mediengruppe/Desktop/tests"
    filenames  = glob.glob(path + "/*.csv.gz")
    
        
    for filename in filenames:
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

            counter = 0
            for column in tags_df.columns:
                print(column)
                print(tags_df[column][1])
                break
            

            

            
 




