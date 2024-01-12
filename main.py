import requests
from requests.exceptions import RequestException
from urllib.parse import urlencode
import json
from urllib.parse import urlparse
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime, timedelta
import time
import re
from requests.auth import HTTPBasicAuth
from spelling import start_spell

def get_formatted_date():
    # Get the current date
    current_date = datetime.now()
    
    # Format the date as "13-November-2023"
    formatted_date = current_date.strftime("%d-%B-%Y")
    
    # Extract the month as "November"
    month_name = current_date.strftime("%B")
    
    # Extract the day as an integer
    day_number = current_date.day
    
    # Extract the year as an integer
    year_number = current_date.year
    
    # Return the formatted date, month name, day number, and year number
    return formatted_date, month_name, day_number, year_number

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

def query_black_table(connection,sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        black_list=[]
        
        #print("Total number of entries in 'black' table:", cursor.rowcount)
        for row in rows:
            black_list.append(row[1].encode().decode('utf-8'))
        return black_list
    except Error as e:
        print(f"Failed to retrieve data from 'black' table: {e}")
    finally:
        if cursor is not None:
            cursor.close()

def get_current_report(connection, report_id):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM reports WHERE id = %s", (report_id,))
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

        # Convert list of dictionaries to JSON string
        json_report = json.dumps(main_report, indent=2)
        return main_report

    except Error as e:
        print(f"Failed to retrieve data from 'report' table: {e}")
        return None
    finally:
        if cursor is not None:
            cursor.close()

def fetch_ahrefs_data(api_key, target, columns, filters, country='us', date='2023-11-13', date_compared='2023-11-06', 
                      limit=50, mode='subdomains', order_by='sum_traffic_merged:desc', protocol='both', 
                      volume_mode='monthly'):
    base_url = "https://api.ahrefs.com/v3/site-explorer/organic-keywords"
    
    # Construct the 'select' parameter with the list of columns
    select = ','.join(columns)

    # The 'where' parameter needs to be a JSON string
    where = json.dumps(filters)

    # Construct query parameters
    query_params = {
        'country': country,
        'date': date,
        'date_compared': date_compared,
        'limit': limit,
        'mode': mode,
        'order_by': order_by,
        'protocol': protocol,
        'select': select,
        'target': target,
        'volume_mode': volume_mode,
        'where': where
    }
    
    # Combine the base URL with the encoded query parameters and filters
    complete_url = f"{base_url}?{urlencode(query_params)}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    
    response = requests.get(complete_url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        return response.raise_for_status()

def login_and_send_post_data(login_url, post_url, login_data, post_data,type="post"):
    try:
        # Create a session to persist cookies and headers across requests
        session = requests.Session()


        username=login_data['username']
        password=login_data['password']
        initial_page = session.get(login_url)

        # Send the login POST request with the login data
        login_response = session.get(login_url, auth=HTTPBasicAuth(username, password))


        # Check if the login was successful by looking for a successful status code or a specific element in the response
        if login_response.ok:
            print("Login successful. Proceeding to send POST data.")

            # Now that you're logged in, send the POST request with 'c' data
            if type == "get":
                post_response = session.get(post_url, data=post_data)
            else:
                post_response = session.post(post_url, data=post_data)

            # Check if the POST request was successful
            if post_response.ok:
                print("POST request successful.")
                return post_response.text
            else:
                print(f"POST request failed with status code: {post_response.status_code}")
                return None
        else:
            print(f"Login failed with status code: {login_response.status_code}")
            return None

    except RequestException as e:
        print(f"An error occurred: {e}")
        return None

def load_json_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        return None
    except json.JSONDecodeError:
        print(f"The file {file_path} does not contain valid JSON.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_report_id(market):
    # Usage example
    file_path = f'markets/{market}.json'  # Replace with your actual file path
    json_data = load_json_from_file(file_path)

    encoded_brands_data = json.dumps(json_data)
    post_data = {
        'c': market,
        'brands':encoded_brands_data
    }
    report_id=login_and_send_post_data(login_url, post_url, login_data, post_data)
    return report_id,json_data

def accumulate_traffic(data,brand,id,black_lists,black_list_folder,misspells=False):

    def sort_subdomains_by_visibility_change(json_data, top_n=5):

        subdomains = json_data

        # Sort the subdomains by the absolute value of their visibility change
        sorted_subdomains = sorted(subdomains.values(), key=lambda x: abs(x['visibility']), reverse=True)

        # Get the top N subdomains with the highest change
        top_subdomains = sorted_subdomains[:top_n]

        return top_subdomains

    def extract_url_components(url):
        parsed_url = urlparse(url)
        subdomain = parsed_url.netloc
        path = parsed_url.path.strip('/')
        return subdomain, path

    def detect_language_codes(path):
        # Regular expression pattern to match a two-letter language code
        # at the beginning of the path, optionally followed by a hyphen
        # and another two-letter code, followed by a slash or end of string
        pattern = r'^([a-z]{2}(?:-[a-z]{2})?)(?=/|$)'
        
        # Find a match in the path
        match = re.match(pattern, path, re.IGNORECASE)
        
        # Return the match if found
        return match.group(0) if match else None

    traffic_summary = {
        'main_vis':0,
        'brand':brand,
        'report':id,
        'subdomains': {},
        'paths': {},
        'queries':{}
    }
    prev_v=get_previous_vis(connection,market,brand,id)

    for entry in data:
        subdomain, path = extract_url_components(entry['best_position_url'])
        language_code = detect_language_codes(path)

        if language_code:
            # If a language code is detected, use the entire path
            final_path_d = path.split('/')

            if len(final_path_d) > 1:
                final_path = final_path_d[0]+"/"+final_path_d[1]
            else:
                final_path = final_path_d[0]

        else:
            # If no language code, print the first part of the path
            final_path = path.split('/')[0]


        path=final_path

        traffic_diff = entry['sum_traffic'] - entry['sum_traffic_prev']
        keyword=entry['keyword_merged']
        if misspells:
            if keyword in misspells['results']:
                print(keyword)
                continue

        url=entry['best_position_url']
        position=entry['best_position']
        prev_pos=entry['best_position_prev']
        sv=entry['volume_merged']
        trend=position - prev_pos
        

            
        main_v=int(prev_v)+int(traffic_diff)
        traffic_summary['main_vis'] = main_v

        blkf=0
        blk=0

        flunq=f'{path}{brand}'

        subname=subdomain
        subunq=f'{subname}{brand}{month_name}{day_number}{year_number}'

        if keyword in black_lists:
            blk=1

        if flunq in black_list_folder:
            blkf=1
    
        if "." in subdomain:
            subdomain_host=subdomain.split(".")[0]

        # Accumulate the traffic differences for subdomains
        
        if subdomain not in traffic_summary['subdomains']:
            traffic_summary['subdomains'][subdomain] = {
                'name':subname,
                'brand':brand,
                'visibility':0,
                'report':id,
                'unq':subunq,
                'date':formatted_date,
                'alias':subdomain
            }
        traffic_summary['subdomains'][subdomain]['visibility'] += traffic_diff

        # Accumulate the traffic differences for paths
        if path not in traffic_summary['paths']:
            unqf=f'{path}{brand}{month_name}{day_number}{year_number}'
            traffic_summary['paths'][path] = {
                'visibility':0,
                'blacklist':blkf,
                'url':url,
                'trend':0,
                'date':formatted_date,
                'unq':unqf,
                'report':id,
                'folder':path,
                'live':"yes"
                }
        traffic_summary['paths'][path]['trend'] += traffic_diff

        

        if trend > 0:
            type="lose"
        else:
            type="win"

        query_item={
            'keyword':keyword,
            'url':url,
            'pos':position,
            'trend': trend,
            'type':type,
            'sv':sv,
            'unq':f'{keyword}{url}{brand}{month_name}{day_number}{year_number}{type}',
            'unqU': f'{url}{brand}',
            'unqA': f'{keyword}{brand}',
            'folder':path,
            'subdomain':subdomain_host,
            'black':blk,
            'date':formatted_date,
            'traffic_diff':traffic_diff
        }

        traffic_summary['queries'][keyword]=query_item
        
    traffic_summary['subdomains']= sort_subdomains_by_visibility_change(traffic_summary['subdomains'], top_n=5)

    return traffic_summary

def read_json_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        return None
    except json.JSONDecodeError:
        print(f"The file {file_path} does not contain valid JSON.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def start(brand, report_id, local_file): 
    black_lists=query_black_table(connection,"SELECT * FROM blacklist")
    black_list_folder=query_black_table(connection,"SELECT * FROM blacklistf")
    file_path = local_file  # Replace this with the path to your JSON file
    json_data = read_json_from_file(file_path)
    start_spell(json_data,report_id,brand)

    traffic_summary = accumulate_traffic(json_data['keywords'],brand,report_id,black_lists,black_list_folder)
    return traffic_summary

def start_ahrefs(api_key,target,market):

    def create_dates():
        # Get today's date
        today = datetime.now()

        # Calculate the date 7 days ago
        seven_days_ago = today - timedelta(days=7)

        return today.strftime('%Y-%m-%d'),seven_days_ago.strftime('%Y-%m-%d')

    today,last_week=create_dates()
    print(today,last_week)

    market=market.lower()
    if market == "uk":
        market = 'gb'
    print(f'Market {market}')
    columns = [
        'keyword_merged', 'sum_traffic','volume_merged', 'sum_traffic_prev', 
        'best_position', 'best_position_prev', 'sum_traffic_merged',
        'best_position_url'
    ]

    if market == 'us':
        sv_filter=499
    else:
        sv_filter=49
    top_pos=20
    traffic_filter=20

    www_filter={
                    "or": [
                        {"field": "best_position_url_raw_prev", "is": ["isubstring", "www"]},
                        {"field": "best_position_url_raw", "is": ["isubstring", "www"]}
                    ]
                }
    www_filter2={
                "or": [
                    {"not": {"field": "best_position_url_raw_prev", "is": ["isubstring", "help"]}},
                    {"field": "best_position_url_raw_prev", "is": "is_null"}
                ]
            }
    www_filter3={
                "or": [
                    {"not": {"field": "best_position_url_raw", "is": ["isubstring", "help"]}},
                    {"field": "best_position_url_raw", "is": "is_null"}
                ]
            }

    filters = {
        "and": [
            {
                "or": [
                    {"field": "best_position_diff", "is": ["gt", 0]},
                    {"field": "best_position_diff", "is": ["lt", 0]}
                ]
            },
            {
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "ddy"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "god"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "sign"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "coupon"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "login"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "outlook"]
                }
            },{
                "not": {
                    "field": "keyword_merged",
                    "is": ["isubstring", "download"]
                }
            },
            {
            "or": [
                {"field": "best_position", "is": ["lte", 20]},
                {
                    "and": [
                        {"field": "best_position_prev", "is": ["lte", 20]},
                        {"field": "best_position", "is": ["gte", 20]}
                    ]
                }
            ]
             },
            {"field": "volume_merged", "is": ["gte", sv_filter]},
            {"field": "best_position_kind", "is": ["eq", "organic"]},
            {"field": "sum_traffic_merged", "is": ["gte", traffic_filter]},
            {"field": "sum_traffic", "is": ["gte", traffic_filter]}
        ]
    }

    if market == 'us':
        filters['and'].append(www_filter)
        filters['and'].append(www_filter2)
        filters['and'].append(www_filter3)
    data = fetch_ahrefs_data(api_key, target, columns, filters, market,today,last_week)
    json_data = json.dumps(data, indent=4)
    with open(file_name, 'w+') as f:
        f.write(json_data)

    return file_name

def generate_sql_inserts(json_data):

    data = json_data

    brand = data['brand']
    
    main_vis = data['main_vis']
    report_id = data['report']
    paths = data['paths']
    queries= data['queries']
    subdomains = data['subdomains']
    inserts = []



    sql = (
        "INSERT INTO `visibility` "
        "(brand, device, date, unq, value, report) "
        "VALUES (%s, %s, %s, %s, %s, %s);"
    )

    deksm="mobile"
    deksd="desktop"
    unqM=f'{brand}{month_name}{day_number}{year_number}{deksm}'
    unqD=f'{brand}{month_name}{day_number}{year_number}{deksd}'
    inserts.append([sql,(brand, deksm, formatted_date, unqM, main_vis, report_id)])
    inserts.append([sql,(brand, deksd, formatted_date, unqD, main_vis, report_id)])

    for folder, details in paths.items():
        url = details['url']
        trend = details['trend']
        visibility = details['visibility']
        date = details['date']
        unq = details['unq']
        folder = details['folder']
        

        # Create the SQL insert statement for each path
        sql = (
            "INSERT INTO `folders` "
            "(url, brand, trend, visibility, date, unq, report, folder) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        )

        # Append to the list of inserts
        inserts.append([sql,(url, brand, trend, visibility, date, unq, report_id, folder)])
    
    for keyword, details in queries.items():
        url = details['url']
        pos = details['pos']
        trend = details['trend']*-1
        date = details['date']
        unq = details['unq']
        unqU = details['unqU']
        unqA = details['unqA']
        folder = details['folder']
        svolume = details['sv']
        black = details['black']
        type_keyword = details['type']  # 'type' is a reserved keyword in Python, so I used 'type_keyword'
        subdomain = details['subdomain']
        traffic_diff = details['traffic_diff']

        # Create the SQL insert statement for each query
        sql = (
            "INSERT INTO `wandl` "
            "(keyword, brand, url, pos, trend, date, unq, unqA, unqU, report, type, folder, black, semrush, svolume, subdomain, traffic) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"
        )

        # Append to the list of inserts
        inserts.append([sql,(keyword, brand, url, pos, trend, date, unq, unqA, unqU, report_id, type_keyword, folder, black, 0, svolume, subdomain,traffic_diff)])
    nn=0
    for details in subdomains:
        nn=nn+1
        visibility = details['visibility']
        date = details['date']
        alias = details['alias']
        unq = details['unq']
        name = details['name']

        # Create the SQL insert statement for each subdomain
        sql = (
            "INSERT INTO `subdomain` "
            "(name, brand, visibility, date, unq, report, alias) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        new_n=f's{nn}'

        # Append to the list of inserts
        inserts.append([sql,(new_n, brand, visibility, date, unq, report_id, alias)])
    
    return inserts

def delete_report(id):
    endoint=f'https://seotoolz4life.com/reports/delete/delete.php?id={id}'
    post_data={}
    login_and_send_post_data(login_url,endoint,login_data,post_data,'get')

def start_api(id):
    create_task1=f'https://seotoolz4life.com/reports/data/API/json.php?id={id}'
    create_task2=f'https://seotoolz4life.com/reports/data/API/json2.php?id={id}'
    get_rank1=f'https://seotoolz4life.com/reports/data/API/jsond.php?id={id}'
    get_rank2=f'https://seotoolz4life.com/reports/data/API/jsond2.php?id={id}'
    start_screen1=f'https://www.seotoolz4life.com/reports/edit/start_screens.php?id={id}'
    start_screen2=f'https://www.seotoolz4life.com/reports/edit/start_screens2.php?id={id}'

    endpoints1=[create_task1,create_task2]
    endpoints=[get_rank1,get_rank2,start_screen1,start_screen2]

    for endpoint in endpoints1:
        print(endpoint)
        post_data={}
        login_and_send_post_data(login_url,endpoint,login_data,post_data,'get')

    time.sleep(120)

    for endpoint in endpoints:
        print(endpoint)
        post_data={}
        login_and_send_post_data(login_url,endpoint,login_data,post_data,'get')

def get_previous_vis(connection,market,brand,id):
    sel_get_last_id=f'SELECT * FROM `reports` where id < {id} AND country LIKE "{market}" ORDER by id desc'
    
    try:
        cursor = connection.cursor()
        cursor.execute(sel_get_last_id)
        rows = cursor.fetchall()

        
        # Decode bytearray to string if necessary
        decoded_row = [col.decode('utf-8') if isinstance(col, bytearray) else col for col in rows[0]]
        last_id=decoded_row[0]

    except Error as e:
        print(f"Failed to retrieve data from 'report' table: {e}")
    finally:
        if cursor is not None:
            cursor.close()

    sqllastv = f'SELECT * FROM `visibility` where report LIKE {last_id} AND brand LIKE "{brand}" and device LIKE "desktop"'

    try:
        cursor = connection.cursor()
        cursor.execute(sqllastv)
        rows = cursor.fetchall()

        return(rows[0][5])


    except Error as e:
        print(f"Failed to retrieve data from 'report' table: {e}")
    finally:
        if cursor is not None:
            cursor.close()


### DATA NEEDED ###
api_key = 'emSBL1FqopynqlSpycg84s0yK_LTdx3x_mJ0hUJ3'
login_data = {
    'username': 'uipath',
    'password': '2022GOSearchSEO77#'
}
login_url="https://www.seotoolz4life.com/reports"
post_url="https://www.seotoolz4life.com/reports/new/post.php"


### CONNECT OT THE DB ###
host = '92.205.189.212'
db_name = 'seotoolz_reports'
username = 'seotoolz_reports'
password = '2022GOSearchSEO77#'

connection = connect_to_db(host, username, password, db_name)


### GET CURRENT DATE ###
formatted_date, month_name, day_number, year_number = get_formatted_date()

report_id=None
## HERE WE SHOULD LOOP ### 
markets=["US"]
for market in markets:
    try:
        report_id,market_data=get_report_id(market)
        brands=market_data['settings']['brands']
        for brandX in brands:
            target=brandX['name']
            brand=brandX['brand']            
            ### GET CURRENT DATE ###
            file_name=f'data/{brand}_{target}_{day_number}_{month_name}.json'
            print(report_id,brand,target)
            if os.path.isfile(file_name):
                print("File exists")
            else:
                start_ahrefs(api_key,target,market)
            if os.path.isfile(file_name):
                if connection and report_id != "Error.":
                    #main_report=get_current_report(connection,report_id)
                    final_data=start(brand,report_id,file_name)
                    #json_data = json.dumps(final_data, indent=4)

                    ins=generate_sql_inserts(final_data)
                    for item in ins:
                        sql=item[0]
                        data=item[1]
                        #print(f'SQL {sql} and data: {data}')
                        try:
                            cursor = connection.cursor()
                            cursor.execute(sql, data)
                        except Error as e:
                            print(f"Failed: {e} {sql}")
                        finally:
                            if cursor is not None:
                                cursor.close()
                else:
                    print (f'Connection: {connection} and ReportID: {report_id}')
            
        
            #os.remove(file_name)
        #start_api(report_id)
            
    except Exception as e:
        print (e)
        if report_id != None:
            delete_report(report_id)

connection.close()