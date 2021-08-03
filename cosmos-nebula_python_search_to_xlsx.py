from tqdm import tqdm, trange
import pandas as pd
import numpy as np
import aiomysql
import asyncio
import pymysql
import math
import json
import os

def parser(data:list)->list:
    """This function will recieve the results of the query and do any parsing necessary. """
    hashtags = "trump2020 fightback MAGA voterfraud wwg1wga foxnews newuser parler 2a antifa backtheblue biden deepstate election2020 fakenews fechadocombolsonaro freedom joebiden oann obamagate parlerksa patriots RogerStone saveourchildren stopthesteal tn2 trump trump2020landslide trumpwon truthisfree twexit walkaway 2020election AlexJonesShow america AmericaFirst bidencheated bidencrimefamily bigtech blacklivesmatter blm blmterrorists boycottfoxnews breaking BuildTheWall catholic cnntapes conservatives coronavirus debates2020 democrats democratsaredestroyingamerica dominion donaldtrump draintheswamp echo funnymemes gijoevets hunterbiden hydroxychloroquine kag kag2020 liberalismisamentaldisorder maga2020 meme millionmagamarch None nwo obama parlerconcierge pizzagate potus presidenttrump proudboys q qanon realdonaldtrump releasethekraken resist salcedostorm savethechildren sayit sidneypowell stevebannon stopthecoup techtyranny thesepeoplearesick trump2020landslidevictory trump2q2q trumprally twitter washtimesoped wdshow wethepeople".split()
    user_ids_added = set()
    results = []
    for i in trange(len(data), desc="Parsing"):
        row = data[i]
        hashtag_col = json.loads(row['hashtags']) if row['hashtags'] else []
        if any(hashtag in row['body'] for hashtag in hashtags) or any(hashtag in hashtag_col for hashtag in hashtags):
            if row['user_id'] in user_ids_added:
                continue
            else:
                user_ids_added.add(row['user_id'])
                results.append(row)
        else:
            continue
    return results


def main():
    conn = Connection()
    '''
    #Copy this code for each query you want to run. Just replace fname, query, and data

    conn.add_task(Query(
        fname="",
        database="",
        query=""" """,
        data=None
    ))
    
    '''


    conn.add_task(Query(
        fname="Domestic Extremism project - Parler",
        database="parler",
        query="""select parler_users_2.*, parler_data_2.body, parler_data_2.hashtags
from parler_users_2
join parler_data_2
on creator_id = user_id
""",
        data=None
    ))



    conn.run(debug=False)


class Query:
    def __init__(self, fname, database, query, data=None, group=""):
        self.fname = fname
        self.database = database
        self.query = query
        self.data = data
        self.group = group


class Connection:
    def __init__(self, export_dir="export"):
        self.pool = None
        self.database = None
        self.tasks = []
        self.export_dir = export_dir

    def add_task(self, query: Query):
        query.query = " ".join(query.query.split())
        t = self.create_task(query)
        self.tasks.append(t)
        self.database = query.database

    async def create_task(self, query: Query):
        results = await self.exectue(query.query, query.data)
        results = parser(results)
        if len(results) > 900000: #chunking files too large
            # df = pd.DataFrame(results)
            num_chunks = math.ceil(len(results) / 900000)
            chunks = np.array_split(results, num_chunks)
            c_count = 1
            for chunk in tqdm(chunks, desc=query.fname+" chunking"): 
                self.save_xlsx(list(chunk), query.fname + '_chunk_{}'.format(c_count))
                c_count += 1
            # del df 
        elif results: self.save_xlsx(results, query.fname)
        else: return

    def run(self, debug=False):
        asyncio.run(self._run_all(), debug=debug)

    async def _run_all(self):
        self.pool = await aiomysql.create_pool(host='144.167.35.121', port=3306, db=self.database,
                                      user='db_exporter', password='Cosmos1', maxsize=20)
        [await f for f in tqdm(asyncio.as_completed(self.tasks),
            total=len(self.tasks), desc="SQL")]
        self.pool.close()
        await self.pool.wait_closed()

    def save_xlsx(self, data, fname, loc="export", cols=[]):
        df = pd.DataFrame(data)
        if cols: df.columns = cols
        path = os.path.join(os.getcwd(), loc+"\\"+fname+".xlsx")
        with pd.ExcelWriter(path, engine='xlsxwriter', options={'strings_to_urls': False}) as writer: 
            df.to_excel(writer, header=True, index=False, encoding='utf-8', na_rep='None')
        del df
        return fname

    async def exectue(self, query, data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if data: await cur.execute(query, data)
                    else: await cur.execute(query)
                    records = await cur.fetchall()
            conn.close()
            return records
        except (AttributeError, pymysql.err.OperationalError):
            #asyncio.exceptions.IncompleteReadError: X bytes read on a total of Y expected bytes
            print("\nFailed to recieve all the data from the db. Re-running the query as blocking.")
            return self.block_execute(query, data)

    def block_execute(self, query, data):
        connection = pymysql.connect(host='144.167.35.121', user='db_exporter', 
            password='Cosmos1', cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            if data: cursor.execute(query, data)
            else: cursor.execute(query)
            records = cursor.fetchall()
        connection.close()
        return records

if __name__ == "__main__":
    main()