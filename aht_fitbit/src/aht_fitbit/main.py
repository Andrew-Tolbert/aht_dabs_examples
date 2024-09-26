from pyspark.sql import SparkSession, DataFrame
import json
import csv
import datetime as dt
import requests
import json 
import time
from xml.etree import ElementTree as ET
from pyspark.sql.functions import explode, col, regexp_replace,to_json
# RELEVANT TIME VARS FOR INCREMENTAL AND BATCH LOADS
from dateutil.relativedelta import relativedelta

client_id = '2395FL' 
base_url = 'https://api.fitbit.com/'
vol = '/Volumes/ahtsa/fitbit' 

def get_vars():
   return {
        'client_id ': client_id,
        'base_url':base_url,
        'vol': vol
    }

def get_taxis(spark: SparkSession) -> DataFrame:
  return spark.read.table("samples.nyctaxi.trips")

# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
  try:
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()

def main():
  get_taxis(get_spark()).show(5)


date_0 = dt.date.today()
date_1 = (date_0 - dt.timedelta(days = 1))
date_7 = (date_0 - dt.timedelta(days = 7))
# for bulk data 
date_120 = (date_0 - dt.timedelta(days = 150))
date_monthStart = (date_0.replace(day=1))
date_5Mo = date_monthStart - relativedelta(months = 5)

# LOAD CREDS FROM JSON
def get_creds():
    with open(f'{vol}/_configs/creds.json') as f:
      JSONData = json.load(f)
    access_token = JSONData['access_token']
    refresh_token = JSONData['refresh_token']
    user_id = JSONData['user_id']
    return {
        'access_token': access_token,
        'refresh_token':refresh_token,
        'user_id': user_id
    }
  
  
def refresh_access_token(creds): 

  url = f"{base_url}oauth2/token"
  
  payload = {
      'refresh_token': creds['refresh_token'],
      'grant_type': 'refresh_token', 
      'client_id': client_id
  }
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
  response = requests.request("POST", url, headers=headers, data=payload)
  
  #REWRITE TOKEN FILE
  with open(f'{vol}/_configs/creds.json', 'w') as json_file:
      json.dump(json.loads(response.text), json_file)
      print('rewrote file')

def get_sleep(creds,date): 
    url = f"{base_url}1.2/user/{creds['user_id']}/sleep/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {creds['access_token']}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "sleep_" + date.replace("-","") + ".json"
        return filename,response.text,err
    except:
        err += 1 
        return 'NA','NA',err
      
def get_activities(creds,date): 
    url = f"{base_url}1/user/{creds['user_id']}/activities/date/{date}.json"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {creds['access_token']}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "activities_" + date.replace("-","") + ".json"
        return filename,response.text,err
    except:
        err += 1 
        return 'NA','NA',err

def get_activityLogList(creds,date,limit): 
    """
    THIS API OPERATES WITH A START DATE AND A LIMIT 
    EX - START ON 2022-09-20 AND GO TO 2022-09-30
    https://dev.fitbit.com/build/reference/web-api/activity/get-activity-log-list/
    11/18/23 - Return Max Date within the file
    """
    
    url = f"{base_url}1/user/{creds['user_id']}/activities/list.json?afterDate={date}&sort=asc&offset=0&limit={limit}"
    
    payload={
        }
    headers = {
      'Authorization': f"Bearer {creds['access_token']}"
    }
    
    err = 0
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()
        filename = "activityLog_" + date.replace("-","") + ".json"               
        return filename,response.text,err
      
    except:
        err += 1 
        return 'NA','NA',err
      
def getTCX(creds,url):
    payload={
        }
    headers = {
      'Authorization': f"Bearer {creds['access_token']}"
    }
    err=0
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
      response.raise_for_status()
      filename = url[-15:-4] + '.xml'
    except:
      err += 1 
      return 'NA','NA',err
    return response.text,filename,err

def s3_interval_update(creds,endpoint,start,end,sleep):
    while start <= end:
      _day = start.strftime('%Y-%m-%d')
      match endpoint:
        case 'sleep':
            filename,data,err = get_sleep(_day,creds['access_token'])
        case 'activities':
            filename,data,err = get_activities(_day,creds['access_token'])
        case _:
            print("Provide a valid service to call")
      if err == 0:
          write_json(f'raw_fitbitapi/{endpoint}',filename,data) 
          #print(f"{filename}")
          print(f"{filename}")
          time.sleep(sleep)
          start += dt.timedelta(days=1)
      else: 
          print('Error Authenticating with API')
          break


def s3_interval_activities(creds,start,end,limit,sleep):
    buffer = 4
    while start <= end:
      _day = start.strftime('%Y-%m-%d')
      filename,data,err = get_activityLogList(_day,creds['access_token'],limit) 
      if err == 0:
          write_json(f'raw_fitbitapi/activitylog',filename,data) 
          print(f"{filename}")
          time.sleep(sleep)
          start += dt.timedelta(days=(limit-buffer))
          
          #NOW GET TCX DATA FROM RECENTLY WRITTEN JSON 
          write_json('temp','temp_json',data)
          tcxDf = spark.read.json(f'{vol}/raw_fitbitapi/activitylog/{filename}')

          if "tcxLink:" in tcxDf.schema.simpleString(): 
            tcxDf= (tcxDf.select("activities")
                  .select("*", explode(tcxDf.activities).alias("activities_exploded"))
                  .select('activities_exploded.tcxLink')
                  .withColumn('tcxLink',regexp_replace('tcxLink', 'user/-/', f"user/{creds['user_id']}/"))
            )

            tcxList = tcxDf.select("tcxLink").distinct().collect()

            for x in tcxList:
              tcxResponse,tcxFilename,tcxErr = getTCX(x[0])
              if tcxErr ==0:
                write_xml('raw_fitbitapi/TCX',tcxFilename,tcxResponse)
                print(tcxFilename)
                time.sleep(3)
              if tcxErr ==1:
                continue 
            #DONE WRITING TCX
      else: 
          print('Error Authenticating with API')
          break

def write_json(path,filename,data):
    """
    writes data to volume
    """
    with open(f'{vol}/{path}/{filename}', 'w') as json_file:
      json.dump(json.loads(data), json_file)

def write_xml(path,filename,data):
    """
    writes data to volume
    """
    with open(f'{vol}/{path}/{filename}', 'w') as xml_file:
      xml_file.write(data)

if __name__ == '__main__':
  main()