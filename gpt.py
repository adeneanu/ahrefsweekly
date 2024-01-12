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

with open('cj.json') as f:
    cj = json.load(f)


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
    #print(json.dumps(data))
    raw_response = requests.post(endpoint, timeout=300, data=json.dumps(data), headers=headers)
    status_code=raw_response.status_code
    print(status_code,raw_response.text)
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

def get_summary(connection, report_id,brand):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT keyword,brand,url,pos,trend,comment,date,type,report,folder,iscom,preview,svolume,fs,foundURL,traffic FROM wandl WHERE report = %s and black = 0 and brand like %s", (report_id,brand,))
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

def get_prev_data(connection,brand):
    try:
        cursor = connection.cursor()
        import datetime

        # Calculate the date 5 weeks ago from today
        five_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=10)
        formatted_date = five_weeks_ago.strftime('%Y-%m-%d')  # Format the date in a way that SQL will understand

        # Execute the query with the additional date filter
        cursor.execute("""
            SELECT keyword, brand, url, pos, trend, comment, date, type, report, folder, iscom, preview, svolume, fs, foundURL
            FROM wandlarch
            WHERE brand = %s
            AND black = 0
            AND iscom = 1
            AND date < %s ORDER BY `id` DESC
        """, (brand, formatted_date))
        
        
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

def calculate_fluctuation_score(ranks_i):
    #ranks = [int(i) for i in ranks]
    ranks=[]
    for i in ranks_i:
        if i != '':
            ranks.append(int(i))
    # Calculate standard deviation as a measure of volatility
    std_dev = np.std(ranks)
    std_dev_factor = std_dev / (max(ranks) - min(ranks)) if max(ranks) != min(ranks) else 0
    
    # Calculate the total number of directional changes
    directional_changes = np.diff(ranks)
    changes_count = sum(np.abs(np.diff(np.sign(directional_changes)))) // 2
    
    # Normalize the number of changes by the length of the series
    changes_factor = changes_count / (len(ranks) - 2) if len(ranks) > 2 else 0

    # Calculate the slope of the line of best fit
    x = np.arange(len(ranks))
    y = np.array(ranks)
    slope, _ = np.polyfit(x, y, 1)
    slope_factor = 1 - min(1, abs(slope) / (max(y) - min(y)) if max(y) != min(y) else 0)

    # The fluctuation score is a combination of the factors
    fluctuation_score = (std_dev_factor + changes_factor + slope_factor) / 3

    # Scale to [0, 100]
    fluctuation_score *= 100
    
    # Clamp the score between 0 and 100
    fluctuation_score = max(0, min(100, fluctuation_score))
    
    return fluctuation_score

def get_blk_fols(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT code FROM blacklistf")
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

def start_script(report_id,brand,seed):
    wls=get_summary(connection,report_id,brand)
    prev_data=get_prev_data(connection,brand)
    blk_fols=get_blk_fols(connection)

    blk_fols_list=[]

    for blkfol in blk_fols:
        if brand in blkfol['code']:
            blk_fols_list.append(blkfol['code'].replace(brand,''))

    ## GET PREVIOUS RANKING
    prev_d={}

    for p in prev_data:
        kw=p['keyword']
        pos=p['preview']
        date=p['date']
        if '&' not in pos and pos != "":
            pos=f'On {date} was ranking on position #{pos}'
            if kw in prev_d:
                prev_d[kw].append(pos)
            else:
                prev_d[kw]=[pos]

    # GET TOKEN
    tokendetails=get_jwt()
    jwt_token=tokendetails['token']
    #print(f'token details: {tokendetails}')

    # INITIATE OPENAI SETTINGS
    model1='gpt-4-1106-preview'
    model2='gpt-3.5-turbo-16k'
    model='gpt-4-32k'

    now = datetime.now()

    # Format the date to 'Day-Month-Year'
    formatted_date = now.strftime("%d-%B-%Y")
    
    summ=[]
    cjs=[]
    fss1=[]
    fols = []
    winners=[]
    losers=[]
    for query in wls:
        stop=0
        if brand == query['brand']:
            iscom=query['iscom']
            fsfs=query['fs']
            
            keyword=query['keyword'].replace("'","`")
            brand=query['brand']
            url=query['url']
            pos=query['pos']
            trend=query['trend']
            comment=query['comment']
            date=query['date']
            type=query['type']
            report=query['report']
            folder=query['folder']
            if folder == "":
                folder="/ Root Folder"
            if folder not in blk_fols_list:
                fols.append(folder)
            preview=query['preview']
            if not preview:
                preview=101                
            svolume=query['svolume']
            fs=query['fs']
            foundURL=query['foundURL']
            traffic=query['traffic']

            
            
            last_pos=int(pos)+int(trend)
            if preview and last_pos:
                trend=int(preview)-int(last_pos)

            else:
                trend=0
    

            item={}
            item['iscom']=iscom
            item['keyword']=keyword
            item['last_position']=last_pos
            item['live_position']=preview
            item['trend_since_last_week']=trend    
            item['type']='loser' if trend > 0 else 'winner'
            item['svolume']=svolume
            
            #item['search_volume']=svolume
            item['url']=url
            if url in cj['urls']:
                if int(preview) < 15 or last_pos < 15:
                    item['cj']=True
                else:
                    item['cj']=False
            else:
                item['cj']=False
            item['is_featured_snippet']=True if fs == 1 else False
            if fs == 1:
                fss1.append(item)

            item['folder']=folder
            #item['traffic_change']=traffic

            if not preview:
                item['foundURL']=foundURL

            if keyword in prev_d:
                item['last_positions']=prev_d[keyword]
            else:
                item['last_positions']=[]



            if iscom == None:
                if url in cj['urls']:
                    if int(preview) < 15 or last_pos < 15:
                        item['cj']=True
                    summ.append(item)
            else:
                if last_pos != preview and stop ==0 and folder not in blk_fols_list:
                    kwf=f'Keyword: {keyword} - SV: {svolume} - folder: {folder}'
                    if trend >0:
                        losers.append(kwf)
                    else:
                        winners.append(kwf)
                    summ.append(item)
                else:
                    if url in cj['urls']:
                        if int(preview) < 15 or last_pos < 15:
                            item['cj']=True
                        summ.append(item)

           
    
    

    
   
    # Count the occurrences of each folder
    folder_counts = Counter(fols)
    # Get the top 5 most common folders
    top_5_folders = folder_counts.most_common(6)
    # Extract just the folder names from the top 5
    top_5_folder_names = [folder for folder, count in top_5_folders]
    # Now top_5_folder_names contains only the top 5 most added folders
    fols = top_5_folder_names

    intro={}
    main={}   
    main2={}   
    for item in summ:
        if item['cj']==True:
            if item not in cjs:
                cjs.append(item)

        
        iscom=item['iscom']
        cjm=item['cj']
     
        if cjm:
            cjm=". It is a Blog Customer Journey article!"
        else:
            cjm="."
        keyword=item['keyword']
        
        last_pos=item['last_position']
        preview=item['live_position']
        trend=item['trend_since_last_week']    
        type=item['type']
        sv=item['svolume']
        item['last_positions']=item['last_positions'][0:10]
        last_ranks=str(item['last_positions'])
        
        #svolume=item['search_volume']
        url=item['url']
        is_fs=item['is_featured_snippet']
        fsms=""

        if is_fs==True:
            fsms=". It is a Featured Snippet (FS)!"
        folder=item['folder']
        if folder == "":
            folder="/ Root Folder"
        #item['traffic_change']=traffic
       


        if not preview:
            foundURL=item['foundURL']
        if preview==1:
            bg="big "
        else:
            bg=""
            if last_pos == 1:
                bg="big "
        if iscom=="1" or item['cj']==True or is_fs:

      
            if folder in fols or cjm or is_fs:
               
                if folder not in main:
                    main[folder]=[]
                    intro[folder]={"winners":0,"losers":0,"trend":0}
                intro[folder]['trend']=intro[folder]['trend']+trend*-1
                if (preview!=last_pos and (trend !=0 or is_fs)) or cj or is_fs:
                    if type=="winner":
                        intro[folder]['winners']=intro[folder]['winners']+1
                        if len(item['last_positions'])>3 and sv > 400:
                            main[folder].append(f'| `{keyword}` - search volume: {sv} is a {bg}winner for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Winning trend of {trend}{fsms}{cjm}. This is its recent historical ranks called prev_list: {last_ranks}\n')
                        else:
                            main[folder].append(f'| `{keyword}` - search volume: {sv} is a {bg}winner for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Winning trend of {trend}{fsms}{cjm}\n')
                    else:
                        intro[folder]['losers']=intro[folder]['losers']+1
                        if len(item['last_positions'])>3 and sv > 400:
                            main[folder].append(f'| `{keyword}` - search volume: {sv}- is a {bg}loser for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Losing trend of {trend}{cjm}. This is its recent historical ranks called prev_list: {last_ranks}\n')
                        else:
                            main[folder].append(f'| `{keyword}` - search volume: {sv}- is a {bg}loser for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Losing trend of {trend}{cjm}\n')    
            else:
                if folder not in main2:
                    main2[folder]=[]
                    intro[folder]={"winners":0,"losers":0,"trend":0}
                intro[folder]['trend']=intro[folder]['trend']+trend*-1
                if preview!=last_pos and (trend !=0 or is_fs):
                    if type=="winner":
                        intro[folder]={"winners":0,"losers":0,"counter":0}
                        if len(item['last_positions'])>3 and sv > 499:
                            main2[folder].append(f'| `{keyword}` - search volume: {sv} - is a {bg}winner for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Winning trend of {trend}{fsms}{cjm}. This is its recent historical ranks called prev_list: {last_ranks}\n')
                        else:
                            main2[folder].append(f'| `{keyword}` - search volume: {sv} - is a {bg}winner for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Winning trend of {trend}{fsms}{cjm}\n')
                    else:
                        intro[folder]['losers']=intro[folder]['losers']+1
                        if len(item['last_positions'])>3 and sv > 499:
                            main2[folder].append(f'| `{keyword}` - search volume: {sv} - is a {bg}loser for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Losing trend of {trend}{cjm}. This is its recent historical ranks called prev_list: {last_ranks}\n')
                        else:
                            main2[folder].append(f'| `{keyword}` - search volume: {sv} - is a {bg}loser for the `{folder} folder` with url: {url}. Is ranking on {preview} and used to rank on {last_pos} last week. Losing trend of {trend}{cjm}\n')
            

    folder_text=""
    rest_text=""
    for m,v in main.items():

        ws=intro[m]['winners']
        ls=intro[m]['losers']
        trd=intro[m]['trend']
        
        folder_text+=(f'\nFolder reports for today - {formatted_date}: Folder {m} - folder trend: {ws} winners vs. {ls} losers - Main Ranking Changes:\n')
        for v in v:
            folder_text+=v

    for m,v in main2.items():
        rest_text+=(f'\nFolder reports for today - {formatted_date}: Folder {m} - Main Ranking Changes:\n')
        for v in v:
            rest_text+=v

    
    system="You are an SEO data analyst. The final goal is to analyse and summarize text & JSON with Google ranking changes - winners (keywords with negative trend) and losers (keyword with positive trend). Be extra careful with decline trends and point to anything unusual. Queries are separated by | "

    template=("Learn this template structure (we will use later as a model not as a 1to1 copy): Main Folders Performance\n\n*placeholder* Folder ({}): Significant improvements were observed with {} jumping from #2 to the #1 spot, {} moving up to #3 from #4, and {} holding steady at #2. The folder benefits from a range of products, as evidenced by {} climbing to #5 from #6 and {} improving to #3 from #4. However, {} saw a slight drop from #2 to #3.\n\n*placeholder*  Folder ({}): The folder shows mixed results. {} improved from #{higher number} to #{lower number}, while {} became a featured snippet (FS). On the downside, {} fell from #3 to #6, and {} dropped slightly to #9 from #8.\n\n*placeholder*  Folder ({}): The {} keyword made a notable leap from #6 to #3. However, {} lost ground, slipping from #5 to #6.\n\n*placeholder*  Folder ({}): A major win with {} soaring to the top position from #6.\n\n*placeholder*  Folder ({}): Positive movement for {}, which climbed from #7 to #3.\n\n*placeholder* Folder ({}): A minor setback with {} dropping to #3 from #2.\n\nFeatured Snippets\n\n{} is now ranking at #1 and has earned a featured snippet status.\n\n Customer Journey:\n  - We rank on #{} for {} with article {} \n - We rank on #{} for {} with article {} \n\nRanking Trends\n\n{} has shown a consistent upward trend, now at #{} previously ranking on #{} 2 weeks ago and #{} 10 weeks ago.\n\n{} has seen a dramatic rise from positions fluctuating around #3 for the past 3 weeks to now holding the top spot.\n\n{} has been relatively steady, currently at #3 up from #8 10 weeks ago.\n\n{} has been stable in the top 5, now at #4.\n\nSEO Modifiers\n\nWinners: Terms with cheap, free, and buy search modifiers have shown strong upward trends, Here are some examples: {},{},{}.\n\nLosers: Terms with online, cheapest search modifiers have shown declining trends, Here are some examples: {},{},{}\n\nHelp and Blog Articles\n\nThis Help article {url of help article} is ranking for {keyword}, currently at position #{} up from #{}.\n\nThis blog article {url of Blog/Resource article} is ranking for {keyword}, currently at position #{rank}.\n\nConclusion\n\nThe analysis indicates that the *placeholder* folder is performing well with multiple products contributing to its success, particularly with keywords reaching the top spots. The *placeholder* folder is showing mixed results but includes a featured snippet win. *placeholder* and *placeholder* folders are experiencing positive trends. Noteworthy is the high movement of {}, ranking with the homepage. Help and Blog/Resource articles are also ranking for specific keywords. Overall, there's a healthy mix of winners across different folders, with some keywords showing significant improvements in rankings. - This was the template. Return only yes if you learned it.")
    
    step_one="""
        SOURCE DATA: {{folder_text}}
        As an SEO analyist, create a report using the provided ranking logic (low position is good, high position is bad) and data, following these detailed instructions:
        IMPORTANT: In the **Main Folders Performance** - Check overall trend (number of winners vs number of losers) for a folder (increased/decreased - based on the number of keywords with a search volume > 500) and then look for a pattern for that given increase/decrease. If there is no clear pattern, please look for queries that: lost/got the #1 spot or went out/in of Top 10 or if nothing goes in one of these categories - look for main keywords highly relevant to the folder.
        IMPORTANT: In the **Main Folders Performance** - Include all big winners/losers: every keyword that reached 1st spot or entered top 5 or top 10 or moved more than 3 spots up in the SERP.\n
        IMPORTANT: Resources is also a blog section.\n
        IMPORTANT: Focus on both winners and losers but pay special attention to losers. We need to spot any issue we might have, any declining trends.
        Main Sections:
        1. **Main Folders Performance**: Mention overall trend (number of winners vs number of losers) for top 5 folders (increased/decreased - based on the number of keywords with a search volume > 500). Outline ranking changes for main folders (winners and losers), noting significant movements, and specify if a folder's improvement is attributed to a single product or a range of products. Do not mention traffic or clicks.
        3. **SEO Modifiers**: Detect and analyze on main search modifier/groups winners and search modifier/groups losers, detect them from the keywords on your own, identifying patterns in query trends related to specific search modifiers/groups (e.g., cheap, free, build). For each modifier, list at least 3 queries with their associated search modifiers, emphasizing significant changes. If a modifier does not have 3 queries in its list, don't add it to this section.  Don't contradict yourself. If a modifier is marked as a positive trend, cannot be in the losing trend also and vice-versa.
        4. **Customer Journey**: Add one more section to the report and list in this section ALL the keywords that are Customer Journey articles that are in top 10 or entered or exited top 10 - marked as 'It is a Blog Customer Journey article!' in the source data, and their ranking changes. Mention only the URL, previous rank and current rank. Again, REPORT ALL of them that are in top 10 or entered or exited top 10! Also, all URLs are blog not HELP urls!
        5. **Featured Snippets**: Add one more section to the report and list in this section ALL the keywords that are Featured Snippets their ranking changes. Mention only the URL, previous rank and current rank. Mark ONLY keywords that have obtained featured snippet status NOT customer journey ones, don't mess them up: 'It is a Featured Snippet (FS)' with (FS) throughout the report.
        
        IMPORTANT: Report any query that existed top 10.
        IMPORTANT: Are you sure you included all important folders with chanages (hosting, domains, websites, etc)?
        IMPORTANT: Highlight if a keyword ranking improvement is associated with a Help or Blog/Resource article instead of the main website page, mentioning the specific URLs.
        IMPORTANT: Mention keywords where the homepage ranks instead of a specific product or folder page.
        IMPORTANT: Don't contradict yourself. If a modifier or folder or keyword is marked as a positive trend, cannot be in the losing trend also and vice-versa.
        IMPORTANT: Differentiate between product and folder, eg: Don`t point to domain related queries that the homepage is ranking for as queries the domain folder is ranking for.
        IMPORTANT: For the **SEO Modifiers** section, ONLY return it if there is at least one modifier that has at least 3 examples!!!!
        Ensure all sections are clearly differentiated using valid Markdown format. Keywords and queries should also be distinctly formatted. The conclusion should be succinct yet comprehensive, enabling a non-technical stakeholder to grasp the key patterns and insights from the data.
        Remember to exclude redundant information or introductions, focus on the data points, and address each point with precision. The report should offer a concise and insightful overview of the ranking changes, featured snippets, and SEO search modifiers based on the provided data.
        """
    step_two="""
        RETURN FULL PREVIOUS REPORT and add make the following updates to it, if needed, but pay extra attention to the instructions:
        IMPORTANT!
        1. Add one more section before conclusion, called **Ranking Trends**: 
        In this section, we'll assess keyword ranking trends based on the 'prev_list'. We're looking for consistent upward (high to low ranks) or downward (low to high ranks) trends over time. Make sure you pay attention to the dates.
        No 'prev_list' means no historical data for that keyword, hence, no analysis. We won't report minor rank changes (1-2 places) unless it is a solidified position in top 5 and historical ranks have been bigger than 8.
        
        A good example: `{keyword} has shown Trends over time, with a recent upward trend. It improved to #{rank1} from #{rank2} last week, and it has seen various positions such as #{rank3} in November 2023, #{rank4} in September 2023, and #{rank5} in early 2021.`
        Significant, consistent changes demonstrate clear trends.         
        2. Have you included all the big trends in the **Ranking Trends* section and marked them correctly? If not update it please! Make sure you are using the 
        Recheck and return full updated report with NO other commentary, with conclusion and every other section from the previous response but updated if needed.
        3. Include all give dates and ranks in your assesment.   
        4. Don't include the ones that have been ranking on the same or -+1/2 positions for a long time, more than 6 months. Example, if a query is ranking on 7,8,9 since two years ago, we don't care. That's is a stable rank.
        5. Before finalizing the report, please confirm that all historical rankings and trends match the data provided based on the following data, don`t output the confirmation do it for your use only, return only the updated report: RECHECK SOURCE DATA.
        """
    step_three="""
        In the **Ranking Trends** section, please double check if all trends are marked corectly - keep in mind, if older rank is bigger than recent one, that's a bad trend, loser/downward trend. Smaller current rank/number than oldest one, is a good trend/winner/upward trend.
        Also, make sure all dates match the ones in the source data. You must include all give dates and ranks in your assesment, and always mention/include the oldest date with its rank, the last item in prev_list. 
        What is the oldest date with ranking for each keyword? Make sure you know it and use it, don't output it, the last item in prev_list.
        Remember smaller rank number is always better/stronger than bigger rank number.Example: 2 is better than 5, and 3 is worse than 2.       
        Return the FULL report with the updated Ranking Trends sections with no other commentary or notes from your part."""
    step_four="""
        IMPORTANT: Report any query that existed top 10.
        IMPORTANT: Have you added all queries marked as FS in the Featured Snippets section?
        IMPORTANT: Are you sure all Customer Journey articles are in the section that are in top 10 or entered or exited top 10? Don't add non-Customer Journey URLs in Customer Journey section. Are you sure are all correctly present in the CJ list and not made up - all entries in the SOURCE DATA with 'It is a Blog Customer Journey article!' ?
        IMPORTANT: Please carefully analyze the provided data and pay special attention to any recurring modifiers or groups within the keywords. Ensure that you identify and report on any trends associated with these modifiers, especially if there are multiple instances that indicate a pattern, such as a winning or losing trend. For example, if there are several keywords related to 'free' services showing a downward trend, include this as a 'loser' in the SEO Modifiers/Groups section. Cross-reference all keywords with their respective modifiers to confirm that no significant trends are missed. Provide a full report with updated sections based on these findings, and double-check that all data points are accurately reflected in the report."
        Return the FULL report with the updated (if needed) sections with no other commentary or notes from your part. RECHECK SOURCE DATA.
        """
    
    
    Xstep_one="""
        
        You have been tasked with creating a detailed SEO ranking report. Your report should have clearly defined sections and adhere to the following guidelines:

        1. For each main folder, provide a brief narrative that includes the overall trend, total number of winners versus losers, and highlights ALL significant ranking changes. Use Markdown headings for each folder, and denote the overall trend in parentheses.
        2. In the Customer Journey section, list the keywords along with their previous and current ranks. Do not include the phrase 'It is a Blog Customer Journey article!' as this is already understood from the section title.
        3. For each folder, include all ranking changes, prioritizing keywords that exited the top 10 or experienced significant rank changes.

        Please ensure that each section of the report is precise and adheres to these instructions, using the template as a guideline but not as a rigid structure.
        Use clear Markdown headings for each section and provide a concise commentary on the data. 
        Begin your assessment and report generation now, adhering to the source data and these instructions.
        Read this SOURCE DATA carefully, then follow the instructions above: {{folder_text}}
    """

    Xstep_two="""
        UPDATE REPORT WITH RANKING TRENDS:

        Revise the existing report to include or update the **Ranking Trends** section by following these instructions:

        1. Identify and document significant ranking trends for each keyword, including both the previous and current ranks. Focus on movements that show a clear trend over time.
        2. Be consistent in your analysis; avoid listing the same modifiers as both winners and losers. If a modifier appears to have conflicting trends, examine the data to determine its overall trend and categorize it accordingly.
        3. Confirm that your analysis is based on the provided 'prev_list' and that it aligns with the data.

        Update the report with these adjustments, ensuring that the ranking trends are depicted accurately and that inconsistencies are resolved.
        """

    Xstep_three="""

        ENSURE ACCURATE REPRESENTATION OF TRENDS:

        In the **Ranking Trends** section, ensure accuracy and consistency:

        1. Verify that upward trends are shown as a movement to lower numerical ranks (e.g., from #10 to #3) and that downward trends are shown as a movement to higher numerical ranks (e.g., from #3 to #10).
        2. Rectify any instances where modifiers are listed as winners and losers simultaneously. Each modifier should be clearly categorized as either showing an overall winning or losing trend based on the majority of keyword data.

        Proceed to reassess and correct the **Ranking Trends** section, focusing on accurately representing each keyword's movement in the rankings and resolving any inconsistencies with modifier categorization.
                """


    Xstep_four="""
        FINAL REPORT COMPLETION AND ACCURACY CHECK:

            Review and update the report with focused verifications to ensure completeness and accuracy:

            1. Confirm that all relevant queries that exited the top 10 are reported with both previous and current ranks.
            2. In the Featured Snippets section, accurately list all queries marked as FS with their correct ranking details.
            3. In the Customer Journey section, list only the previous and current ranks for each keyword without any additional labels since the section title already identifies them as CJ articles.
            4. For the SEO Modifiers section, ensure that no modifiers are duplicated across winners and losers. If a modifier has conflicting data, analyze the overall impact to determine its true trend. Include at least 3 examples for each modifier. If not, do not mention it at all.
            5. Revisit the Main Folders Performance section and provide a narrative that includes the total number of winners versus losers, and describe the main changes with previous and current ranks for each keyword.

            Ensure the final report is concise, informative, and formatted correctly in Markdown. The conclusion should succinctly summarize the key findings, focusing on the most impactful data without extraneous details.

            Follow these instructions closely to ensure that the report is comprehensive, accurate, and adheres to the specified format.
            Return the FULL updated report not just the updated parts and make the conclussion brief, mentioning all main key points from the report. Do not mention anywhere that this is the final revision or anything along these lines.
        """

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
    


    data['prompts'].append({"from":"system","content":system.replace("'","`")})
    data['isTemplate']=True
    folder_text=folder_text.replace("'","`")
    rest_text=rest_text.replace("'","`")
    data["props"]={'folder_text':folder_text,'rest_text':rest_text}

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
    print('S1')
    
    r1,pid,rid=step(data,step_one)
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})

    print('S2')
    r1,pid,rid=step(data,step_two)
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})
    print('S3')
    r1,pid,rid=step(data,step_three)
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})

    r1,pid,rid=step(data,step_four)

    """
    data['prompts'].append({"from":"assistant","content":r1.replace("'","`")})
    print('S2')
    retry=0
    try:
        r1,pid,rid=step(data,step_two)
    except:
        retry=1
    
    if retry==1:
        data['providerOptions']['frequency_penalty']=0
        data['providerOptions']['presence_penalty']=0
        data['providerOptions']['model']=model1
        r1,pid,rid=step(data,step_two)
    """
    furl=f'https://caas.godaddy.com/prompt/view?promptId={pid}&resultId={rid}'
    print(seed,furl)

    return r1

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

def get_all_reports(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM reports WHERE live = 2")
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

def start(connection,report,brand,seed):
    summar=start_script(report,brand,seed)

    if connection.is_connected():
        unq=str(report)+brand
        cursor = connection.cursor()
        cursor.execute("SET NAMES utf8mb4;")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        query = "INSERT INTO summaries (report, brand, summar, unq) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE summar = VALUES(summar)"
        cursor.execute(query, (report,brand,summar,unq,))
        connection.commit()
        print("Text data inserted successfully")

### CONNECT OT THE DB ###
host = '92.205.189.212'
db_name = 'seotoolz_reports'
username = 'seotoolz_reports'
password = '2022GOSearchSEO77#'
connection = connect_to_db(host, username, password, db_name)


all_live_reports=get_all_reports(connection)

for reports in all_live_reports:

    report=reports['id']
    country=reports['country']
    if report==101554:
        file_path = f'markets/{country}.json'  # Replace with your actual file path
        json_data = load_json_from_file(file_path)
        brands=json_data['settings']['brands']
        for brandX in brands:
            
            target=brandX['name']
            brand=brandX['brand'] 
            print(brand,report,country)
            seed = random.randint(950, 1000)
            seed=973

            start(connection,report,brand,seed)

"""
seed=973
report="101553"
brand="GDDE"
start(connection,report,brand,seed)        
"""
connection.close()