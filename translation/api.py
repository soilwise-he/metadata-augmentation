import flask
from flask import request
import requests;
import json; 


# Load environment variables from .env file
load_dotenv()

correlationMap = {}

@app.route('/translate/{lang}/{string}', methods=['GET'])
def translate(lang,string):
    # query database on existance of string in language

    # if yes, return it

    # if not, insert it to be translated


@app.route('/requestTrans', methods=['GET'])
def requestTrans():

    # query database for pending translations

    # for each string to be translated, make a request

    #from .env
    eTranslationRestUrl = "https://webgate.ec.europa.eu/etranslation/si/translate"
    applicationName = "*******"
    password = "*******"

    translationRequest = {}
    translationRequest['sourceLanguage'] = sourceLanguage
    translationRequest['targetLanguages'] = [targetLanguage]
    translationRequest['callerInformation'] = {"application" : applicationName, "username":"John Smith"}
    translationRequest['textToTranslate'] = textToTranslate
    translationRequest['requesterCallback'] = f'https://{domain}/callback

    jsonTranslationRequest = json.dumps(translationRequest)

    jsonHeader = {'Content-Type' : 'application/json'}

    response = requests.post(eTranslationRestUrl, auth=HTTPDigestAuth(applicationName, password), headers = jsonHeader, data=jsonTranslationRequest)  

    requestId = response.text

    print("Request ID:" + requestId )

    if(requestId > 0)
        correlationMap[response.text] = "" 

    # insert the ticket to the database, to identify which string to be updated

    return response.text # ticket


@app.route('/callback', methods=['POST'])
def callback():
    requestId = request.form.get('request-id')
    targetLanguage = request.form.get('target-language')
    translatedText = request.form.get('translated-text')
    print('Request ID: ' + requestId + ", Target language: " + targetLanguage + ", Translated text:" + translatedText)
    correlationMap[requestId] = translatedText

    # update string in translation table with updated value (remove ticket)

    return "OK"

