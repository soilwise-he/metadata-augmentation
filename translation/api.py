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

LanguagePairs = ["AR-BG","AR-CS","AR-DA","AR-DE","AR-EL","AR-EN","AR-ES","AR-ET","AR-FI","AR-FR","AR-GA","AR-HR","AR-HU","AR-IS","AR-IT","AR-JA","AR-LT","AR-LV","AR-MT","AR-NB","AR-NL","AR-NN","AR-PL","AR-PT","AR-RO","AR-RU","AR-SK","AR-SL","AR-SV","AR-TR","AR-UK","AR-ZH","BG-AR","BG-CS","BG-DA","BG-DE","BG-EL","BG-EN-QE","BG-ES","BG-ET","BG-FI","BG-FR","BG-GA","BG-HR","BG-HU","BG-IS","BG-IT","BG-JA","BG-LT","BG-LV","BG-MT","BG-NB","BG-NL","BG-NN","BG-PL","BG-PT","BG-RO","BG-RU","BG-SK","BG-SL","BG-SV","BG-TR","BG-UK","BG-ZH","CS-AR","CS-BG","CS-DA","CS-DE","CS-EL","CS-EN-QE","CS-ES","CS-ET","CS-FI","CS-FR","CS-GA","CS-HR","CS-HU","CS-IS","CS-IT","CS-JA","CS-LT","CS-LV","CS-MT","CS-NB","CS-NL","CS-NN","CS-PL","CS-PT","CS-RO","CS-RU","CS-SK","CS-SL","CS-SV","CS-TR","CS-UK","CS-ZH","DA-AR","DA-BG","DA-CS","DA-DE","DA-EL","DA-EN-QE","DA-ES","DA-ET","DA-FI","DA-FR","DA-GA","DA-HR","DA-HU","DA-IS","DA-IT","DA-JA","DA-LT","DA-LV","DA-MT","DA-NB","DA-NL","DA-NN","DA-PL","DA-PT","DA-RO","DA-RU","DA-SK","DA-SL","DA-SV","DA-TR","DA-UK","DA-ZH","DE-AR","DE-BG","DE-CS","DE-DA","DE-EL","DE-EN-QE","DE-ES","DE-ET","DE-FI","DE-FR","DE-GA","DE-HR","DE-HU","DE-IS","DE-IT","DE-JA","DE-LT","DE-LV","DE-MT","DE-NB","DE-NL","DE-NN","DE-PL","DE-PT","DE-RO","DE-RU","DE-SK","DE-SL","DE-SV","DE-TR","DE-UK","DE-ZH","EL-AR","EL-BG","EL-CS","EL-DA","EL-DE","EL-EN-QE","EL-ES","EL-ET","EL-FI","EL-FR","EL-GA","EL-HR","EL-HU","EL-IS","EL-IT","EL-JA","EL-LT","EL-LV","EL-MT","EL-NB","EL-NL","EL-NN","EL-PL","EL-PT","EL-RO","EL-RU","EL-SK","EL-SL","EL-SV","EL-TR","EL-UK","EL-ZH","EN-AR","EN-BG-QE","EN-CS-QE","EN-DA-QE","EN-DE-QE","EN-EL-QE","EN-ES-QE","EN-ET-QE","EN-FI-QE","EN-FR-QE","EN-GA-QE","EN-HR-QE","EN-HU-QE","EN-IS","EN-IT-QE","EN-JA","EN-LT-QE","EN-LV-QE","EN-MT-QE","EN-NB","EN-NL-QE","EN-NN","EN-PL-QE","EN-PT-QE","EN-RO-QE","EN-RU","EN-SK-QE","EN-SL-QE","EN-SV-QE","EN-TR","EN-UK","EN-ZH","ES-AR","ES-BG","ES-CS","ES-DA","ES-DE","ES-EL","ES-EN-QE","ES-ET","ES-FI","ES-FR","ES-GA","ES-HR","ES-HU","ES-IS","ES-IT","ES-JA","ES-LT","ES-LV","ES-MT","ES-NB","ES-NL","ES-NN","ES-PL","ES-PT","ES-RO","ES-RU","ES-SK","ES-SL","ES-SV","ES-TR","ES-UK","ES-ZH","ET-AR","ET-BG","ET-CS","ET-DA","ET-DE","ET-EL","ET-EN-QE","ET-ES","ET-FI","ET-FR","ET-GA","ET-HR","ET-HU","ET-IS","ET-IT","ET-JA","ET-LT","ET-LV","ET-MT","ET-NB","ET-NL","ET-NN","ET-PL","ET-PT","ET-RO","ET-RU","ET-SK","ET-SL","ET-SV","ET-TR","ET-UK","ET-ZH","FI-AR","FI-BG","FI-CS","FI-DA","FI-DE","FI-EL","FI-EN-QE","FI-ES","FI-ET","FI-FR","FI-GA","FI-HR","FI-HU","FI-IS","FI-IT","FI-JA","FI-LT","FI-LV","FI-MT","FI-NB","FI-NL","FI-NN","FI-PL","FI-PT","FI-RO","FI-RU","FI-SK","FI-SL","FI-SV","FI-TR","FI-UK","FI-ZH","FR-AR","FR-BG","FR-CS","FR-DA","FR-DE","FR-EL","FR-EN-QE","FR-ES","FR-ET","FR-FI","FR-GA","FR-HR","FR-HU","FR-IS","FR-IT","FR-JA","FR-LT","FR-LV","FR-MT","FR-NB","FR-NL","FR-NN","FR-PL","FR-PT","FR-RO","FR-RU","FR-SK","FR-SL","FR-SV","FR-TR","FR-UK","FR-ZH","GA-AR","GA-BG","GA-CS","GA-DA","GA-DE","GA-EL","GA-EN","GA-ES","GA-ET","GA-FI","GA-FR","GA-HR","GA-HU","GA-IS","GA-IT","GA-JA","GA-LT","GA-LV","GA-MT","GA-NB","GA-NL","GA-NN","GA-PL","GA-PT","GA-RO","GA-RU","GA-SK","GA-SL","GA-SV","GA-TR","GA-UK","GA-ZH","HR-AR","HR-BG","HR-CS","HR-DA","HR-DE","HR-EL","HR-EN-QE","HR-ES","HR-ET","HR-FI","HR-FR","HR-GA","HR-HU","HR-IS","HR-IT","HR-JA","HR-LT","HR-LV","HR-MT","HR-NB","HR-NL","HR-NN","HR-PL","HR-PT","HR-RO","HR-RU","HR-SK","HR-SL","HR-SV","HR-TR","HR-UK","HR-ZH","HU-AR","HU-BG","HU-CS","HU-DA","HU-DE","HU-EL","HU-EN-QE","HU-ES","HU-ET","HU-FI","HU-FR","HU-GA","HU-HR","HU-IS","HU-IT","HU-JA","HU-LT","HU-LV","HU-MT","HU-NB","HU-NL","HU-NN","HU-PL","HU-PT","HU-RO","HU-RU","HU-SK","HU-SL","HU-SV","HU-TR","HU-UK","HU-ZH","IS-AR","IS-BG","IS-CS","IS-DA","IS-DE","IS-EL","IS-EN","IS-ES","IS-ET","IS-FI","IS-FR","IS-GA","IS-HR","IS-HU","IS-IT","IS-JA","IS-LT","IS-LV","IS-MT","IS-NB","IS-NL","IS-NN","IS-PL","IS-PT","IS-RO","IS-RU","IS-SK","IS-SL","IS-SV","IS-TR","IS-UK","IS-ZH","IT-AR","IT-BG","IT-CS","IT-DA","IT-DE","IT-EL","IT-EN-QE","IT-ES","IT-ET","IT-FI","IT-FR","IT-GA","IT-HR","IT-HU","IT-IS","IT-JA","IT-LT","IT-LV","IT-MT","IT-NB","IT-NL","IT-NN","IT-PL","IT-PT","IT-RO","IT-RU","IT-SK","IT-SL","IT-SV","IT-TR","IT-UK","IT-ZH","JA-AR","JA-BG","JA-CS","JA-DA","JA-DE","JA-EL","JA-EN","JA-ES","JA-ET","JA-FI","JA-FR","JA-GA","JA-HR","JA-HU","JA-IS","JA-IT","JA-LT","JA-LV","JA-MT","JA-NB","JA-NL","JA-NN","JA-PL","JA-PT","JA-RO","JA-RU","JA-SK","JA-SL","JA-SV","JA-TR","JA-UK","JA-ZH","LT-AR","LT-BG","LT-CS","LT-DA","LT-DE","LT-EL","LT-EN-QE","LT-ES","LT-ET","LT-FI","LT-FR","LT-GA","LT-HR","LT-HU","LT-IS","LT-IT","LT-JA","LT-LV","LT-MT","LT-NB","LT-NL","LT-NN","LT-PL","LT-PT","LT-RO","LT-RU","LT-SK","LT-SL","LT-SV","LT-TR","LT-UK","LT-ZH","LV-AR","LV-BG","LV-CS","LV-DA","LV-DE","LV-EL","LV-EN-QE","LV-ES","LV-ET","LV-FI","LV-FR","LV-GA","LV-HR","LV-HU","LV-IS","LV-IT","LV-JA","LV-LT","LV-MT","LV-NB","LV-NL","LV-NN","LV-PL","LV-PT","LV-RO","LV-RU","LV-SK","LV-SL","LV-SV","LV-TR","LV-UK","LV-ZH","MT-AR","MT-BG","MT-CS","MT-DA","MT-DE","MT-EL","MT-EN-QE","MT-ES","MT-ET","MT-FI","MT-FR","MT-GA","MT-HR","MT-HU","MT-IS","MT-IT","MT-JA","MT-LT","MT-LV","MT-NB","MT-NL","MT-NN","MT-PL","MT-PT","MT-RO","MT-RU","MT-SK","MT-SL","MT-SV","MT-TR","MT-UK","MT-ZH","NB-AR","NB-BG","NB-CS","NB-DA","NB-DE","NB-EL","NB-EN","NB-ES","NB-ET","NB-FI","NB-FR","NB-GA","NB-HR","NB-HU","NB-IS","NB-IT","NB-JA","NB-LT","NB-LV","NB-MT","NB-NL","NB-NN","NB-PL","NB-PT","NB-RO","NB-RU","NB-SK","NB-SL","NB-SV","NB-TR","NB-UK","NB-ZH","NL-AR","NL-BG","NL-CS","NL-DA","NL-DE","NL-EL","NL-EN-QE","NL-ES","NL-ET","NL-FI","NL-FR","NL-GA","NL-HR","NL-HU","NL-IS","NL-IT","NL-JA","NL-LT","NL-LV","NL-MT","NL-NB","NL-NN","NL-PL","NL-PT","NL-RO","NL-RU","NL-SK","NL-SL","NL-SV","NL-TR","NL-UK","NL-ZH","NN-AR","NN-BG","NN-CS","NN-DA","NN-DE","NN-EL","NN-EN","NN-ES","NN-ET","NN-FI","NN-FR","NN-GA","NN-HR","NN-HU","NN-IS","NN-IT","NN-JA","NN-LT","NN-LV","NN-MT","NN-NB","NN-NL","NN-PL","NN-PT","NN-RO","NN-RU","NN-SK","NN-SL","NN-SV","NN-TR","NN-UK","NN-ZH","PL-AR","PL-BG","PL-CS","PL-DA","PL-DE","PL-EL","PL-EN-QE","PL-ES","PL-ET","PL-FI","PL-FR","PL-GA","PL-HR","PL-HU","PL-IS","PL-IT","PL-JA","PL-LT","PL-LV","PL-MT","PL-NB","PL-NL","PL-NN","PL-PT","PL-RO","PL-RU","PL-SK","PL-SL","PL-SV","PL-TR","PL-UK","PL-ZH","PT-AR","PT-BG","PT-CS","PT-DA","PT-DE","PT-EL","PT-EN-QE","PT-ES","PT-ET","PT-FI","PT-FR","PT-GA","PT-HR","PT-HU","PT-IS","PT-IT","PT-JA","PT-LT","PT-LV","PT-MT","PT-NB","PT-NL","PT-NN","PT-PL","PT-RO","PT-RU","PT-SK","PT-SL","PT-SV","PT-TR","PT-UK","PT-ZH","RO-AR","RO-BG","RO-CS","RO-DA","RO-DE","RO-EL","RO-EN-QE","RO-ES","RO-ET","RO-FI","RO-FR","RO-GA","RO-HR","RO-HU","RO-IS","RO-IT","RO-JA","RO-LT","RO-LV","RO-MT","RO-NB","RO-NL","RO-NN","RO-PL","RO-PT","RO-RU","RO-SK","RO-SL","RO-SV","RO-TR","RO-UK","RO-ZH","RU-AR","RU-BG","RU-CS","RU-DA","RU-DE","RU-EL","RU-EN","RU-ES","RU-ET","RU-FI","RU-FR","RU-GA","RU-HR","RU-HU","RU-IS","RU-IT","RU-JA","RU-LT","RU-LV","RU-MT","RU-NB","RU-NL","RU-NN","RU-PL","RU-PT","RU-RO","RU-SK","RU-SL","RU-SV","RU-TR","RU-UK","RU-ZH","SK-AR","SK-BG","SK-CS","SK-DA","SK-DE","SK-EL","SK-EN-QE","SK-ES","SK-ET","SK-FI","SK-FR","SK-GA","SK-HR","SK-HU","SK-IS","SK-IT","SK-JA","SK-LT","SK-LV","SK-MT","SK-NB","SK-NL","SK-NN","SK-PL","SK-PT","SK-RO","SK-RU","SK-SL","SK-SV","SK-TR","SK-UK","SK-ZH","SL-AR","SL-BG","SL-CS","SL-DA","SL-DE","SL-EL","SL-EN-QE","SL-ES","SL-ET","SL-FI","SL-FR","SL-GA","SL-HR","SL-HU","SL-IS","SL-IT","SL-JA","SL-LT","SL-LV","SL-MT","SL-NB","SL-NL","SL-NN","SL-PL","SL-PT","SL-RO","SL-RU","SL-SK","SL-SV","SL-TR","SL-UK","SL-ZH","SV-AR","SV-BG","SV-CS","SV-DA","SV-DE","SV-EL","SV-EN-QE","SV-ES","SV-ET","SV-FI","SV-FR","SV-GA","SV-HR","SV-HU","SV-IS","SV-IT","SV-JA","SV-LT","SV-LV","SV-MT","SV-NB","SV-NL","SV-NN","SV-PL","SV-PT","SV-RO","SV-RU","SV-SK","SV-SL","SV-TR","SV-UK","SV-ZH","TR-AR","TR-BG","TR-CS","TR-DA","TR-DE","TR-EL","TR-EN","TR-ES","TR-ET","TR-FI","TR-FR","TR-GA","TR-HR","TR-HU","TR-IS","TR-IT","TR-JA","TR-LT","TR-LV","TR-MT","TR-NB","TR-NL","TR-NN","TR-PL","TR-PT","TR-RO","TR-RU","TR-SK","TR-SL","TR-SV","TR-UK","TR-ZH","UK-AR","UK-BG","UK-CS","UK-DA","UK-DE","UK-EL","UK-EN","UK-ES","UK-ET","UK-FI","UK-FR","UK-GA","UK-HR","UK-HU","UK-IS","UK-IT","UK-JA","UK-LT","UK-LV","UK-MT","UK-NB","UK-NL","UK-NN","UK-PL","UK-PT","UK-RO","UK-RU","UK-SK","UK-SL","UK-SV","UK-TR","UK-ZH","ZH-AR","ZH-BG","ZH-CS","ZH-DA","ZH-DE","ZH-EL","ZH-EN","ZH-ES","ZH-ET","ZH-FI","ZH-FR","ZH-GA","ZH-HR","ZH-HU","ZH-IS","ZH-IT","ZH-JA","ZH-LT","ZH-LV","ZH-MT","ZH-NB","ZH-NL","ZH-NN","ZH-PL","ZH-PT","ZH-RO","ZH-RU","ZH-SK","ZH-SL","ZH-SV","ZH-TR","ZH-UK"]
ReduceISOPairs = [    
    {"bg": {"code":"bul","label":"Bulgarian"}},        
    {"fr": {"code":"fre|fra","label":"French"}},            
    {"pl": {"code":"pol","label":"Polish"}},    
    {"cs": {"code":"cze|ces","label":"Czech"}},            
    {"hr": {"code":"hrv","label":"Croatian"}},            
    {"pt": {"code":"por","label":"Portuguese"}},    
    {"da": {"code":"dan","label":"Danish"}},        
    {"hu": {"code":"hun|mag","label":"Hungarian"}},            
    {"ro": {"code":"rum|ron","label":"Romanian"}},    
    {"de": {"code":"ger|deu","label":"German"}},    
    {"is": {"code":"ice|isl","label":"Icelandic"}},        
    {"ru": {"code":"rus","label":"Russian"}},    
    {"et": {"code":"est","label":"Estonian"}},        
    {"it": {"code":"ita","label":"Italian"}},
    {"sk": {"code":"slo|slk","label":"Slovak"}},    
    {"el": {"code":"gre|ell","label":"Greek"}},    
    {"lt": {"code":"lit","label":"Lithuanian"}},    
    {"sl": {"code":"slv","label":"Slovenian"}},    
    {"es": {"code":"spa|esp","label":"Spanish"}},    
    {"lv": {"code":"lav","label":"Latvian"}},    
    {"sv": {"code":"swe","label":"Swedish"}},    
    {"fi": {"code":"fin","label":"Finnish"}},    
    {"nl": {"code":"dut|nld","label":"Dutch"}},
    {"ch": {"code":"chi|zho", "label":"Chinese"}},
    {"ar": {"code":"ara|arb", "label": "Arabic"}}, 
    {"ja": {"code":"jap", "label": "Japanese"}},
    {"ga": {"code":"gle", "label": "Irish"}},
    {"nn": {"code":"nor", "label":"Norwegian"}},
    {"tr": {"code":"tur",  "label":"Turkish"}}
]

class trans(BaseModel):
    lang_source: str
    lang_target: str
    source: str
    context: str

@app.post("/translate") #we use post, to prevent non url safe chars
def translate(tdata: trans):
    lang_source = tdata.lang_source
    lang_target = tdata.lang_target
    source = tdata.source
    tcontext = tdata.context or ''
    ls = None
    ts = None
    
    for k,v in ReduceISOPairs.items():
        if k == lang_source.lower() or lang_source.lower() in v['code']:
            ls = k
        if k == lang_target.lower() or lang_target.lower() in v['code']:
            ts = k

    if not (ls and ts and f"{ls}-{ts}".upper() in LanguagePairs):
        raise HTTPException(status_code=404, detail=f"Translation {ls}-{ts} not available")
    
    # query database on existance of string in language
    myrec = dbQuery("select target, source from harvest.translations where source=%s and lang_source=%s and lang_target=%s;",(source,ls,ts))

    # if yes, return it
    if myrec and len(myrec) > 0: 
        for r in myrec:
            target, source = r 
            return(target)
    else: 
        # if not return default, # todo: insert it, to be translated
        myrec = insertSQL("harvest.translations",['source','lang_source','lang_target','date_inserted','context'],(source,ls,ts,datetime.now(),tcontext))
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
      