
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from requests_toolbelt.multipart import decoder
from google.cloud import storage
import io
import json
import os

def hello_gcs(event, context):
    storage_client = storage.Client()
    file = event
    bucket = storage_client.bucket(file['bucket'])
    blob = bucket.get_blob(file['name'])
    contentType = blob.content_type
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data), sep=',')
    transposeData = pd.DataFrame(df.T)
    tempData = transposeData.reset_index()
    tempData.columns = tempData.iloc[0]
    tempData = tempData.reindex(tempData.index.drop(0)).reset_index(drop=True)  
    tempData.columns.name = None
    final_list = []
    temp_list = []
    i=0
    while i < tempData.index.stop:
        j=0
        dict = {}
        dict_2 = {}
        while j < 5:
            dict[(tempData.loc[i].index[j][:-1]).replace(" ", "")] = str((tempData.loc[i])[j])+""
            dict[(tempData.loc[i].index[j][:-1]).replace(" ", "")] = str((tempData.loc[i])[j])+""
            dict[(tempData.loc[i].index[j][:-1]).replace(" ", "")] = str((tempData.loc[i])[j])+""
            dict[(tempData.loc[i].index[j][:-1]).replace(" ", "")] = str((tempData.loc[i])[j])+""
            dict[(tempData.loc[i].index[j][:-1]).replace(" ", "")] = str((tempData.loc[i])[j])+""
            j = j+1
    k=5
    temp_list = []
    while k<len(tempData.loc[0].index):
        dict_2 = {}
        dict_2["date"] =  str((tempData.loc[0].index[k]))+""
        dict_2["mnemonicValue"] = str((tempData.loc[i])[k])+""
        temp_list.append(dict_2)
        k = k + 1
    dict['data'] = temp_list
    i = i+1
    final_list.append(dict)

    client = bigquery.Client(project="robotic-century-343717")
    table_id = "robotic-century-343717.csv_testing.data_final_table-cf-22"

    records = [
    {
      "Mnemonic": "FIP.IUSA_MABI",
      "Description": "Baseline Scenario (October 2021): Industrial Production: Total, (Index 2017=100, SA)",
      "Source": "U.S. Board of Governors of the Federal Reserve System (FRB); Moody's Analytics Estimated and Forecasted",
      "Native_Frequency": "QUARTERLY",
      "Geography": "Abilene, TX Metropolitan Statistical Area",
      "data": [
           {
             "date" :"_01Jan1970",
             "mnemonicValue":"1.1"
           },
           {
             "date" :"_02Jan1970",
             "mnemonicValue":"2.21"
           },
           {
              "date": "_03Jan1970",
              "mnemonicValue": "3"
           },
           {
              "date": "_04Jan1970",
              "mnemonicValue": "5.21"
           }
        ]  

    }]   

    dataframe = pd.DataFrame(
    final_list,
    columns=[
        "Mnemonic",
        "Description",
        "Source",
        "NativeFrequency",
        "Geography",
        "Data"
    ]
    )
    # dataframe = json.dumps(records).encode('utf-8')
    #json_object = json.loads(dataframe)

    job_config = bigquery.LoadJobConfig(
    #schema=[
    #     bigquery.SchemaField("Mnemonic", "STRING", mode="NULLABLE"),
    #     bigquery.SchemaField("Description", "STRING", mode="NULLABLE"),
    #     bigquery.SchemaField("Source", "STRING", mode="NULLABLE"),
    #     bigquery.SchemaField("NativeFrequency", "STRING", mode="NULLABLE"),
    #     bigquery.SchemaField("Geography", "STRING", mode="NULLABLE"),
    #     bigquery.SchemaField("Data", "RECORD", mode="REPEATED", fields = [
    #         bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
    #         bigquery.SchemaField("mnemonicValue", "STRING", mode="NULLABLE")
    #     ])
    # ],
    write_disposition="WRITE_TRUNCATE"
    )
    #job = client.load_table_from_json(json_object, table_id, job_config = job_config)
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config) 
    job.result()
    table = client.get_table(table_id)

    
    