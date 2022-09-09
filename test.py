import pandas as pd 
import datetime
path = "C:/Users/Julian.Baltzer/OneDrive - FUNKE Mediengruppe/Desktop/Tests.gz"
dataframe = pd.read_csv(path, sep=',',low_memory=False,compression='gzip')

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

dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex=r'^tag')))]
dataframe = dataframe.where((pd.notnull(dataframe)), 0.0)
#dataframe["lineItem/intervalUsageStart"]= pd.to_datetime(dataframe["lineItem/intervalUsageStart"])
#dataframe["lineItem/intervalUsageEnd"]= pd.to_datetime(dataframe["lineItem/intervalUsageEnd"])
counter = 0
for col_name in dataframe.columns: 
    print(col_name + " " +str(counter))
    counter = counter + 1
    
for index, rows in dataframe.iterrows():
    print(rows["q_status"],
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
                                          rows["cost/skuUnitDescription"])
    counter = counter + 1
    
for x in rows:
                query = "INSERT INTO queue VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(query,(0,rows[counter][14],rows[counter][15],rows[counter][16],rows[counter][17],rows[counter][18],rows[counter][19],rows[counter][0],rows[counter][1],rows[counter][2],rows[counter][3],rows[counter][4],rows[counter][5],rows[counter][6],rows[counter][7],rows[counter][8],rows[counter][9],rows[counter][10],rows[counter][11],rows[counter][12]))
                counter = counter + 1