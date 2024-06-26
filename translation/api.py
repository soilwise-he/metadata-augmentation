from fastapi import FastAPI, HTTPException, Form
from typing_extensions import Annotated
from typing import List
from pydantic import BaseModel
import requests, json, os  
from requests.auth import HTTPDigestAuth
from dotenv import load_dotenv
import psycopg2
from datetime import datetime

# Load environment variables from .env file
load_dotenv()
#from .env
eTranslationRestUrl = os.environ.get("TR_SERVICE_URL") or "https://webgate.ec.europa.eu/etranslation/si/translate"
applicationName = os.environ.get("TR_LOGIN")
password = os.environ.get("TR_PASSCODE")
domain = os.environ.get("TR_CALLBACK_DOMAIN")

allowed_languages = os.environ.get("TR_ALLOWED_LANGUAGES") or '*'

# FastAPI app instance
rootpath=os.environ.get("ROOTPATH") or "" 
app = FastAPI(title="Translate app",
    description="an app to translate strings",
    summary="an app to translate strings",
    version="0.0.1",root_path=rootpath)

class trans(BaseModel):
    lang_source: str
    lang_target: str
    source: str

@app.post("/translate") #todo: should we use post, to prevent weird chars?
def translate(tdata: trans):
    lang_source = tdata.lang_source
    lang_target = tdata.lang_target
    source = tdata.source
    # query database on existance of string in language
    myrec = dbQuery("select target, source from harvest.translations where source=%s and lang_source=%s and lang_target=%s;",(source,lang_source,lang_target))

    # if yes, return it
    if myrec and len(myrec) > 0: 
        for r in myrec:
            target, source = r 
            return(target)
    else: 
        # if not return default, # todo: insert it, to be translated
        myrec = insertSQL("harvest.translations",['source','lang_source','lang_target','date_inserted'],(source,lang_source,lang_target,datetime.now()))
        # todo: request translation?
        # requestRecord(lang_source,lang_target,source)
        return(source)

@app.get('/requestTrans')
def requestTrans():

    # query database for pending translations
    myrecs = dbQuery('select source,lang_source,lang_target from harvest.translations where ticket is null and target is null')
    # for each string to be translated, make a request
    for rec in myrecs:
        source,lang_source,lang_target = rec 
        requestRecord(lang_source,lang_target,source)
        
    return "OK"

def requestRecord(lang_source,lang_target,source):

    translationRequest = {}
    translationRequest['sourceLanguage'] = lang_source
    translationRequest['targetLanguages'] = [lang_target]
    translationRequest['callerInformation'] = {"application" : applicationName, "username":"ingest-bot"}
    translationRequest['textToTranslate'] = source
    translationRequest['requesterCallback'] = f'{domain}/callback'

    jsonTranslationRequest = json.dumps(translationRequest)
    jsonHeader = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    requestId = None

    try:
        response = requests.post(eTranslationRestUrl, auth=HTTPDigestAuth(applicationName, password), headers = jsonHeader, data=jsonTranslationRequest)  
        response.raise_for_status()
        requestId = response.text
    except requests.exceptions.HTTPError as err:
        print(err)
        print(response.text)

    if(requestId and requestId not in [None,'']):
        # insert the ticket to the database, to identify which string to be updated
        dbQuery("update harvest.translations set ticket=%s where lang_source=%s and lang_target=%s and source=%s;",(requestId,lang_source,lang_target,source),hasoutput=False)
        return requestId
    else:
        return "Error: No requestId"

@app.post('/callback')
def callback(requestId: Annotated[str, Form(alias="request-id")], 
             targetLanguage: Annotated[str, Form(alias="target-language")], 
             translatedText: Annotated[str, Form(alias="translated-text")]):

    dbQuery("update harvest.translations set target=%s, date_updated=%s where ticket=%s and lang_target=%s;",(translatedText,datetime.now(),requestId,targetLanguage),hasoutput=False)

    # update string in translation table with updated value (remove ticket)

    return "OK"

# Start the application
@app.on_event('startup')
async def startup():
    try:
        dbInit()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database connection failed") from e

def dbInit():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )  

def dbQuery(sql,params=(),hasoutput=True):
    dbconn = dbInit()
    try:
        cursor = dbconn.cursor()
        cursor.execute(sql,params)
        if hasoutput:
            return cursor.fetchall()
        else:
            dbconn.commit()
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        dbconn.close();

def insertSQL(table, fields, values):
    
    sql = f"INSERT INTO {table} ({', '.join(fields)}) values ({','.join(['%s' for x in range(len(fields))])}) ON CONFLICT DO NOTHING;"

    dbconn = dbInit()
    with dbconn.cursor() as cur:
        try:
            # execute the INSERT statement
            cur.execute(sql, values)
            # commit the changes to the database
            dbconn.commit()
        except Exception as e:
            print(f"Error: {str(e)}")
        finally:
            dbconn.close();
      