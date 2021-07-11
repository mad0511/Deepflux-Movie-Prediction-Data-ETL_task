import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import numpy as np
import argparse
import json

class BoxOfficeCollect2DB:

    """
    Get movies data scraped from https://boxofficecollection.in/

    get_total_collection(config)
    Params 
        * config - movie name and release date as key value
    
    df_to_db(df, tblname, isreplace)
        **Note - Any dataframe can be pushed - df column datatype as table datatype**
    Params
        * df - data frame to be inserted to db
        * tblname - table name (default = 'movies_collection')
        * isreplace - (default = False) if True - replace table if already exists, else append data to existing table
    
    collect(cfg_file_path)
    Params
        * cfg_file_path - path for config file with key value pairs
    """
    
    def __init__(self, url):
        self.url = url
    
    def get_total_collection(self, config):
        try:
            # movie name & release date from config
            movie_name = config[0]
            from_date = config[1]
            
            # get html data
            scrape_url = self.url + movie_name.replace(' ','-').lower() + '-box-office-collection-day-wise'
            print(f"Box office collection run for - {scrape_url}")
            html_content = requests.get(scrape_url)
            
            # read html content
            soup_data = BeautifulSoup(html_content.content, 'html5lib')
            txt = soup_data.prettify()

            # read html to df
            print("Read html data as dataframe...")
            tables = pd.read_html(txt)
            box_ofc_df = tables[0]
            box_ofc_df.rename(columns={'Box Office': 'days_from_release', 'Collection':'box_office_collection'}, inplace=True)

            # drop total collection row
            pd.set_option('mode.chained_assignment', None)
            box_ofc_df = box_ofc_df[: -1]
            print("Transforming df - crores in numbers, calc date from release, add movie name")

            # change crores in numbers & add movie name
            box_ofc_df.box_office_collection = box_ofc_df.box_office_collection.str.extract("(\d*\.?\d+)", expand=True)
            box_ofc_df.box_office_collection = box_ofc_df.box_office_collection.astype(float)*100000000
            box_ofc_df.box_office_collection = box_ofc_df.box_office_collection.astype(np.int64)
            box_ofc_df['movie_name'] = movie_name
            
            # date column based on days_from_release
            box_ofc_df['date'] = box_ofc_df.days_from_release.str.split('Day ').apply(lambda x: datetime.strptime(from_date, "%Y-%m-%d").date() + timedelta(days=int(x[-1])) \
                                        if len(x)<=2 else \
                                            str(datetime.strptime(from_date, "%Y-%m-%d").date() + timedelta(days=int(str(x[1]).split('-')[0]))) +' - '+ \
                                                str(datetime.strptime(from_date, "%Y-%m-%d").date() + timedelta(days=int(str(x[-1]).split('-')[-1]))))

            print(f"Extracted Dataframe data- \n{box_ofc_df.head(5)}")
            return box_ofc_df
        
        except Exception as ex:
            print(f"Exception occurred while extracting - {ex}")


    def df_to_db(self, df, tblname="movies_collection", isreplace=False):
        # connect and push df to db
        try:
            conn = sqlite3.connect('movies.db')
            df.to_sql(tblname, conn, if_exists='replace' if isreplace else 'append', index=False)
            print("Successfully pushed df to db")
        
        except Exception as ex:
            print(f"Exception during push to db - {ex}")
    
    
    def collect(self, cfg_file_path):
        with open(cfg_file_path, 'r') as f:
            config = json.load(f)
        
        for mov_name, rel_date in config.items():
            print(f"Movie - {mov_name},\nRelease date - {rel_date}")
            box_ofc_df = self.get_total_collection([mov_name, rel_date])
            self.df_to_db(box_ofc_df)
        

        




if __name__ == '__main__':
    """
    Sample command for execution - 
        python box_ofc_data_collection.py -c {PATH FOR CFG FILE.json}
    """
    parser = argparse.ArgumentParser(description='Scrape Movie Box Office data to DB.')
    parser.add_argument('-c','--cfg', help='Path to config file', required=True)
    args = parser.parse_args()

    # collect scraped data to sqlite db
    BoxOfficeCollect2DB("https://boxofficecollection.in/").collect(args.cfg)