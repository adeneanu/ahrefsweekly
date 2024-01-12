# The endpoint URL for the GoDaddy CaaS API
import requests
import json
import mysql.connector
from mysql.connector import Error
import numpy as np
import random
import json
from collections import Counter
from datetime import datetime


#list_models  https://caas.api.godaddy.com/v1/prompts/models?effort=seodev
#get JWT token https://api.seotoolz4life.com/v1/caas/sso/
#create prompt https://api.seotoolz4life.com/v1/caas/create_prompt/
#get all prompts https://caas.api.godaddy.com/v1/prompts/results/?filter=group:seodev
#get prompt https://caas.api.godaddy.com/v1/prompts/results/?promptId={id}
#get result of prompt https://caas.api.godaddy.com/v1/prompts/results/{id}

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
    raw_response = requests.post(endpoint, timeout=300, data=json.dumps(data), headers=headers)
    status_code=raw_response.status_code
    if raw_response.status_code == 200:
        try:
            return raw_response.json()
        except:
            print(raw_response.text)
    else:
        error_message={'Status_Code':status_code}
        return error_message

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

def get_summary(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT s.brand, s.summar 
            FROM summaries s
            JOIN reports r ON s.report = r.id
            WHERE r.live = 2;""")
        rows = cursor.fetchall()

        # Getting column names from cursor.description
        columns = [col[0] for col in cursor.description]

        # Convert rows to list of dictionaries
        main_report = []
        for row in rows:
            # Decode bytearray to string if necessary
            decoded_row = [col.decode('utf-8') if isinstance(col, bytearray) else col for col in row]
            # Create a dictionary with column names as keys and row data as values
            row_dict = dict(zip(columns, decoded_row))
            main_report.append(row_dict)


        return main_report

    except Error as e:
        print(f"Failed to retrieve data from 'report' table: {e}")
        return None
    finally:
        if cursor is not None:
            cursor.close()


### CONNECT OT THE DB ###
host = '92.205.189.212'
db_name = 'seotoolz_reports'
username = 'seotoolz_reports'
password = '2022GOSearchSEO77#'
connection = connect_to_db(host, username, password, db_name)
def start_script(seed):
    wls=get_summary(connection)
    text=""
    for row in wls:
        brand=row['brand']
        summary=row['summar']
        add=f'Brand: {brand} - Summary: {summary}\n'
        text=text+add
    # GET TOKEN
    tokendetails=get_jwt()
    jwt_token=tokendetails['token']
    #print(f'token details: {tokendetails}')

    # INITIATE OPENAI SETTINGS
    model1='gpt-4-1106-preview'
    model2='gpt-3.5-turbo-16k'
    model='gpt-4-32k'
    
    template="""Learn this template structure (will use later, I will call it example_template). Return only 'YES':
        ### SEO Ranking Changes Summary for all GoDaddy Brands (GDUS, GDMX, GDAU, GDIN, GDCA, GDDE, GDUK, 123REG in UK market, DomainFactory or DF in DE market, HEDE in DE market)
        #### Overview of Main Trends Across All Markets (US, MX, AU, IN, DE, UK, CA)
        **Overview:**
        {overall_summary}
        **Positive Trends:**
        Keywords with modifiers such as `{positive_modifier_1}`, `{positive_modifier_2}`, `{positive_modifier_3}`, etc., are showing upward trends across {all_or_some} markets: {example_1_from_market1},{example_1_from_market2},{example_2_from_market2}
        Certain folders like `{positive_folder_1}`, `{positive_folder_2}`, and `{positive_folder_3}` are experiencing significant improvements with keywords achieving top positions across all markets.
        Customer Journey articles are generally improving in rankings, contributing positively to the `{positive_resources_folder}`'s performance.
        **Negative Trends:**
        Keywords with high search volumes and modifiers like `{negative_modifier_1}`, `{negative_modifier_2}`, `{negative_modifier_3}`, etc., are losing positions across english speaking markets.
        Folders such as `{negative_folder_1}`, `{negative_folder_2}`, and `{negative_folder_3}` are facing declines with several keywords falling out of top positions across all markets.
        **Stable Trends:**
        Some folders like `{stable_folder_1}` and `{stable_folder_2}` are showing stable performance with an equal number of winners and losers.
        **Featured Snippets:**
        No significant gains or losses in Featured Snippets were reported, except for `{featured_snippet_keyword}` maintaining its #1 position with a Featured Snippet in the `{featured_snippet_market}` market.
        #### Market-by-Market Summaries
        **United States (GDUS)**
        Increased: `{us_increased_folder_1}` and `{us_increased_folder_2}` folders with keywords like `{us_increased_keyword_1}` (#1 from #3) and `{us_increased_keyword_2}` (#1 FS).
        Decreased: `{us_decreased_folder_1}` and `{us_decreased_folder_2}` folders with keywords like `{us_decreased_keyword_1}` (#6 from #3) and `{us_decreased_keyword_2}` (out of top 5).
        Stable: `{us_stable_folder}` folder with `{us_stable_keyword_1}` and `{us_stable_keyword_2}` steady on #2.
        SEO Modifiers: `{us_positive_modifier}` are winners; high search volume terms are losers.
        Featured Snippets: {us_fs_keyword_1} reached FS status, up from #2 
        Customer Journey: {us_cj_url_1} entered top 10 for {us_increased_keyword_10} from #15
        **India (GDIN)**
        Increased: `{in_increased_folder_1}`, `{in_increased_folder_2}`, and `{in_increased_folder_3}` folders with keywords like `{in_increased_keyword_1}` (#1 from #3) and `{in_increased_keyword_2}` (#7 from #9).
        Mixed Results: `{in_mixed_folder_1}` and `{in_mixed_folder_2}` folders with `{in_mixed_keyword_1}` (#3 from #4) improving and `{in_mixed_keyword_2}` (#2 from #1) declining.
        SEO Modifiers: `{in_positive_modifier}` and `{in_positive_modifier_2}` are winners; `{in_negative_modifier}` and `{in_negative_modifier_2}` are losers.
        **Canada (GDCA)**
        Increased: `{ca_increased_folder_1}`, `{ca_increased_folder_2}`, and `{ca_increased_folder_3}` folders with `{ca_increased_keyword_1}` (#7 from #5) and `{ca_increased_keyword_2}` (#3 from #9).
        Decreased: `{ca_decreased_folder}` folder with `{ca_decreased_keyword_1}` (#4 from #2) and `{ca_decreased_keyword_2}` (#7 from #3) declining.
        Stable: `{ca_stable_folder}` folder with `{ca_stable_keyword_1}` (#5 slightly up from #6) and `{ca_stable_keyword_2}` (stable on #6).
        SEO Modifiers: `{ca_positive_modifier}` and `{ca_positive_modifier_2}` are winners; `{ca_negative_modifier}` and `{ca_negative_modifier_2}` are losers. 
        **Australia (GDAU)**
        Increased: `{au_increased_folder_1}` and `{au_increased_folder_2}` folders with `{au_increased_keyword_1}` (#1 up from #3) and `{au_increased_keyword_2}` (#5 up from #9).
        Decreased: `{au_decreased_folder_1}` and `{au_decreased_folder_2}` folders with `{au_decreased_keyword_1}` (#5 down from #1) and `{au_decreased_keyword_2}` (#9 down from #6) declining.
        Neutral: `{au_neutral_folder}` folder with `{au_neutral_keyword_1}` (#1) improving and `{au_neutral_keyword_2}` (#7) declining.
        SEO Modifiers: `{au_positive_modifier}` and `{au_positive_modifier_2}` are winners; `{au_negative_modifier}` is a loser.
        **United Kingdom (GDUK)**
        Increased: `{uk_increased_folder_1}` and `{uk_increased_folder_2}` folders with `{uk_increased_keyword_1}` and `{uk_increased_keyword_2}` entering the top 5 from outside the top 100.
        Decreased: `{uk_decreased_folder_1}`, `{uk_decreased_folder_2}`, and `{uk_decreased_folder_3}` folders with `{uk_decreased_keyword_1}` (#10) and `{uk_decreased_keyword_2}` (lost one position) declining.
        Stable: `{uk_stable_folder}` folder with `{uk_stable_keyword_1}` (#1) and `{uk_stable_keyword_2}` (#2).
        SEO Modifiers: `{uk_positive_modifier}` and `{uk_positive_modifier_2}` are winners; `{uk_negative_modifier}` and `{uk_negative_modifier_2}` are losers.
        **United Kingdom (123REG)**
        Increased: `{uk_increased_folder_1}` and `{uk_increased_folder_2}` folders with `{uk_increased_keyword_1}` and `{uk_increased_keyword_2}` entering the top 5 from outside the top 100.
        Decreased: `{uk_decreased_folder_1}`, `{uk_decreased_folder_2}`, and `{uk_decreased_folder_3}` folders with `{uk_decreased_keyword_1}` (#10) and `{uk_decreased_keyword_2}` (lost one position) declining.
        Stable: `{uk_stable_folder}` folder with `{uk_stable_keyword_1}` (#1) and `{uk_stable_keyword_2}` (#2).
        SEO Modifiers: `{uk_positive_modifier}` and `{uk_positive_modifier_2}` are winners; `{uk_negative_modifier}` and `{uk_negative_modifier_2}` are losers.
        **Mexico (GDMX)**
        Increased: `{mx_increased_folder}` with `{mx_increased_keyword_1}` (#1) and `{mx_increased_keyword_2}` (#7).
        Mixed Results: `{mx_mixed_folder}` with `{mx_mixed_keyword_1}` (#8) improving and `{mx_mixed_keyword_2}` (#16) declining.
        Decreased: `{mx_decreased_folder_1}` and `{mx_decreased_folder_2}` with `{mx_decreased_keyword_1}` (#4) and `{mx_decreased_keyword_2}` (#13) declining.
        SEO Modifiers: `{mx_positive_modifier}` and `{mx_positive_modifier_2}` are winners; `{mx_negative_modifier}` and `{mx_negative_modifier_2}` are losers.
        Ranking Fluctuation: Keywords around modifiers like `{mx_mod1}` and `{mx_mod2}` are on a declining trend with more than 3 keywords dropping in ranks each: {mx_mod1_keyword1},{mx_mod1_keyword2},{mx_mod1_keyword3},{mx_mod2_keyword1},{mx_mod2_keyword2},{mx_mod2_keyword3}.
        **Germany (GDDE)**
        Increased: `{de_increased_folder_1}`, `{de_increased_folder_2}`, `{de_increased_folder_3}`, and `{de_increased_folder_4}` folders with `{de_increased_keyword_1}` (#9) and `{de_increased_keyword_2}` (#8).
        Decreased: `{de_decreased_folder_1}`, `{de_decreased_folder_2}`, `{de_decreased_folder_3}`, `{de_decreased_folder_4}`, `{de_decreased_folder_5}`, and `{de_decreased_folder_6}` folders with `{de_decreased_keyword_1}` (#13) and `{de_decreased_keyword_2}` (#8).
        SEO Modifiers: `{de_positive_modifier}` and `{de_positive_modifier_2}` are winners; `{de_negative_modifier}` and `{de_negative_modifier_2}` are losers.
        **Germany (HEDE)**
        Increased: `{de_increased_folder_1}`, `{de_increased_folder_2}`, `{de_increased_folder_3}`, and `{de_increased_folder_4}` folders with `{de_increased_keyword_1}` (#9) and `{de_increased_keyword_2}` (#8).
        Decreased: `{de_decreased_folder_1}`, `{de_decreased_folder_2}`, `{de_decreased_folder_3}`, `{de_decreased_folder_4}`, `{de_decreased_folder_5}`, and `{de_decreased_folder_6}` folders with `{de_decreased_keyword_1}` (#13) and `{de_decreased_keyword_2}` (#8).
        SEO Modifiers: `{de_positive_modifier}` and `{de_positive_modifier_2}` are winners; `{de_negative_modifier}` and `{de_negative_modifier_2}` are losers.
        #### Conclusion
        {conclussion}
        """
    system="You are an SEO data analyst. The final goal is to analyse and summarize text with Google ranking changes."

    data = {
        "prompts": [],
        "provider": "openai_chat",
        "providerOptions": {
            "model": model1,
            "temperature":0.0,
            "frequency_penalty":0,
            "seed":seed,
            "max_tokens":4096,
            "presence_penalty":0
        }
    }
    
    step_one = """
        Use the example_template as a guide but NOT as a 1 to 1 template.
        Provide a concise summary of SEO ranking changes for all the brands in all given markets, NOT only the US, based on the latest reports. 
        Try to spot trends across all markets (not only US) around keyword modifiers, specific products/folders, etc.
        The summary should focus exclusively on observed data changes (across all markets, not only US) without recommendations or general SEO advice. Here are the instructions for the report you will return:
        - All main folders should be analyzed - Domains Folder, - Hosting Folder, - Websites Folder, - Other Folders unless there are no examples to give.
        - ATTENTION! You've been using mostly US data for this section. Try to cover all markets, US, CA, IN, UK, DE, AU, MX.

        **Market-by-Market Summaries:**
        - All main folders should be included in the summary Domains, Hosting, Website - unless there are no examples to give.
        - For each market, summarize the most important changes in SEO rankings. Give at least 2-4 examples to support your statements with current and previous rank.
        - Structure it in `Increased`: - Domains Folder, - Hosting Folder, - Websites Folder, - Other Folders | `Decreased`:  - Domains Folder, - Hosting Folder, - Websites Folder, - Other Folders | `Modifiers` | `Other`| `FS`| `CJ`- in the latter 2, only if changes happened and give at least 3 examples. In `Other` you should list 2-4 nuggets of interesting things you notice for that market. For each folder and modifier give at least 3 keyword ranking changes (previous and current) examples.
        - Include a brief analysis of the key movements observed. Give at least 2-4 examples to support your statements with current and previous rank.
        - Identify consistent trends or anomalies in specific folders or product groups. Give at least 2-4 examples to support your statements with current and previous rank.
        - Include main trends around pages, products, SEO keyword modifiers. Give at least 2-4 examples to support your statements with current and previous rank.

        - **Supporting Data:**
        - Accompany each statement with at least 2-4 examples of specific keyword ranking changes.
        - For each example, mention the keyword, previous rank, and current rank to illustrate the change.

        - **Trends and Observations:** 
        - Identify any overarching trends or patterns seen across multiple markets. Give at least 2-4 examples to support your statements with current and previous rank.
        - Note any significant gains or losses in Featured Snippets, if applicable. Give at least 2-4 examples to support your statements with current and previous rank.


        *IMPORTANT*
        - Start by providing an overview of the main trends in SEO rankings for all brands across all markets. Give examples to support your statements with current and previous rank.
        - Include main trends around pages, products, SEO keyword modifiers. Give at least 2-4 query examples to support your statements with current and previous rank.
        - Present the information in clear, concise markdown format.
        - Ensure each market summary is segregated and easily distinguishable.
        - Base all statements on the actual data from the SEO ranking changes reports.
        - Refrain from including any form of recommendations or advisory comments.
        - In the conclusion, for each idea presented, give at least 2-4 examples to support your statement with current and previous rank.
        - Don't contradict yourself. If a modifier or folder or keyword is marked as a positive trend, cannot be in the losing trend also and vice-versa.

        RETURN ONLY THE FULL REPORT WITH ALL THE MARKETS, MAIN TREND AND WITHOUT ANY OTHER INTRODUCTION OR COMMENTARY!

        Here is the data: {{text}}"""

    data['prompts'].append({"from":"system","content":system.replace("'","`")})
    data['isTemplate']=True

    text=text.replace("'","`")

    data["props"]={'text':text}

    # STEP 1
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

    data['providerOptions']['model']=model1

    r1,pid,rid=step(data,template)
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})

    r1,pid,rid=step(data,step_one)
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})
    print('S1')

    furl=f'https://caas.godaddy.com/prompt/view?promptId={pid}&resultId={rid}'
    print(seed,furl)

    return r1

i=0

while i < 1:
    i=i+1
    
    seed = random.randint(950, 1000)
    brand="ALL"
    seed=973

    summar=start_script(seed)

    if connection.is_connected():
        

        # current date and time
        date_time = datetime.now()

        # format specification
        format = '%Y-%m-%d'

        # applying strftime() to format the datetime
        string = date_time.strftime(format)
        cursor = connection.cursor()
        cursor.execute("SET NAMES utf8mb4;")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        query = "INSERT INTO gd_summaries (brand, summar, unq) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE summar = VALUES(summar)"
        cursor.execute(query, (brand,summar,string,))
        connection.commit()
        print("Text data inserted successfully")

connection.close()