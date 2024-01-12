# The endpoint URL for the GoDaddy CaaS API
import requests
import json
import mysql.connector
from mysql.connector import Error

def start_spell(json_data,report_id,brand):
    def connect_to_db(host, username, password, db_name):
        try:
            connection = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                database=db_name
            )
            if connection.is_connected():
                print("Successfully connected to the database")
                return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None
    
    host = '92.205.189.212'
    db_name = 'seotoolz_reports'
    username = 'seotoolz_reports'
    password = '2022GOSearchSEO77#'
    connection = connect_to_db(host, username, password, db_name)

    private_token="2024@SearchSEO88!@"

    def get_jwt():
        headers={'Token':private_token,'Content-Type': 'application/json; charset=utf-8'}
        endpoint="https://api.seotoolz4life.com/v1/caas/sso/"
        raw_response = requests.post(endpoint, timeout=10, headers=headers)
        status_code=raw_response.status_code
        if raw_response.status_code == 200:
            return raw_response.json()
        else:
            error_message={'Status_Code':status_code}
            return error_message

    def create_prompt(data,jwt_token):
        headers={'Token':private_token,'Jwt':jwt_token,'Content-Type': 'application/json; charset=utf-8'}
        endpoint="https://api.seotoolz4life.com/v1/caas/create_prompt/"
        raw_response = requests.post(endpoint, timeout=60, data=json.dumps(data), headers=headers)
        status_code=raw_response.status_code
        if raw_response.status_code == 200:
            return raw_response.json()
        else:
            error_message={'Status_Code':status_code}
            return error_message

    # GET TOKEN
    tokendetails=get_jwt()
    jwt_token=tokendetails['token']
    #print(f'token details: {tokendetails}')

    # INITIATE OPENAI SETTINGS
    model='gpt-4-32k'
    #model='gpt-3.5-turbo'

    data = {
            "prompts": [],
            "provider": "openai_chat",
            "providerOptions": {
                "model": model
            }
        }


    def return_string(json_data):
        
        # Iterate over each dictionary in the list
        for query in json_data['keywords']:
            # If a "." is present in keyword_merged
            query['keyword_merged']=query['keyword_merged'].replace("'", "")
            if '.' in query['keyword_merged']:
                # Remove the first occurrence of "."
                query['keyword_merged'] = query['keyword_merged'].replace('.', '', 1)

        # Sort the list of dictionaries based on keyword_merged
        json_data['keywords'] = sorted(json_data['keywords'], key = lambda x: x['keyword_merged'])


        st=""
        for query in json_data['keywords']:
            kw=query['keyword_merged']
            sv=query['volume_merged']
            
            if sv > 499:
                add=f'{kw},'
                st=st+add
        return st

    string_list=return_string(json_data)
    # CREATE PROMPTS - EXAMPLE
    system='Given a list of search queries, identify any queries that are likely misspellings of other queries. A misspelling is defined as a query that is recognized as an incorrect variant of a word or phrase.'
    user="""
    Instructions:
    1. Determine if a query is or contains a misspelling. The order of the words in a phrase does not matter. Only return keywords or phrases in which there is an actual misspell. Do not count wrong grammar article or other grammar issues. Only misspells.
    2. Only include the misspelled query in the list, not the correct version and only include ones that you are 100% sure to be a misspell, add `@` and after it list the correct variant.
    3. Detect the language of the majority of the keywords and do not include in the list anything that is in another language.
    4. Do not count 2-3 letter before or after domain or domains.
    5. Provide the final list of identified misspelled queries. Output ONLY the list of misspelled queries comma separated. No other commentary.
    YOU must 100% sure each keyword in your output is a misspell.
        
        """
    data['prompts'].append({"from":"system","content":system})
    data['prompts'].append({"from":"user","content":'List of queries, one per row: '+string_list})
    data['prompts'].append({"from":"user","content":user})

    step_two="Are you sure about your final list? Please return it again but make sure ONLY misspells are in."

    # GET PROMPTS
    #print(json.dumps(get_prompt("prompts"),indent=4))
    #print(json.dumps(get_prompt("result",'5ddef0295e5683f27603a88acccd2628133bc66c645ff0f078c453f4b3a5dcbc'),indent=4))
    def step(data,step):
        data['prompts'].append({"from":"user","content":step.replace("'","`")})
        retry=0
        # CREATE PROMPT
        try:
            response=create_prompt(data,jwt_token)
        except Exception as e:
            retry=1
            print(e)
        if retry==1:
            response=create_prompt(data,jwt_token)
        # GET PROMPT ID

        pid=response['data']['promptId']
        # GET RESULT ID
        id=response['data']['id']
        # GET ANSWER
        result=response['data']['value']['content']

        return result,pid,id
    # CREATE PROMPT
    response=create_prompt(data,jwt_token)

    # GET PROMPT ID
    pid=response['data']['promptId']
    # GET RESULT ID
    id=response['data']['id']
    # GET ANSWER
    result=response['data']['value']['content']

    
    data['prompts'].append({"from":"assistant","content":result.replace("'","`")})
    r1,pid,rid=step(data,step_two)

    if connection.is_connected():
        unq=str(report_id)+str(brand)
        cursor = connection.cursor()
        cursor.execute("SET NAMES utf8mb4;")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        query = "INSERT INTO new_black_list (list, report_id,brand,unq) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE list = VALUES(list)"
        try:
            cursor.execute(query, (r1,report_id,brand,unq,))
            connection.commit()
            print("Text data inserted successfully")
        except:
            print('Could not add misspless into DB')
