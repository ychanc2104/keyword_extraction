import os
import pandas as pd
import datetime
import numpy as np
from gAPI.googleoauth2 import GoogleOAuth2
from gAPI.gads import GoogleAds
from db.mysqlhelper import MySqlHelper
from basic.date import to_datetime, get_date_shift, get_today, datetime_to_str


class GoogleSearchConsole(GoogleOAuth2):

    def update_4db(self, web_id, siteUrl):
        date_start = datetime_to_str(get_date_shift(days=3))
        date_end = datetime_to_str(get_today())
        print(f"update from {date_start} to {date_end}")
        self.save_to_query_table(web_id, date_start, date_end, siteUrl)
        self.save_to_page_query_table(web_id, date_start, date_end, siteUrl)
        self.save_to_page_table(web_id, date_start, date_end, siteUrl)
        self.save_to_device_table(web_id, date_start, date_end, siteUrl)
    ## fetch and save data day by day
    def save_4db_by_date(self, web_id, siteUrl, date_start='2021-01-01', date_end=None):
        if date_end==None:
            date_end = datetime_to_str(get_today())
        num_days = (to_datetime(date_end) - to_datetime(date_start)).days + 1
        date_list = [to_datetime(date_start) + datetime.timedelta(days=x) for x in range(num_days)]
        if num_days==1:
            date_list += date_list
        for i in range(len(date_list)-1):
            date_start = datetime_to_str(date_list[i])
            date_end = datetime_to_str(date_list[i+1])
            print(f"update date range from {date_start} to {date_end}...")
            self.df_search_console_query = self.save_to_query_table(web_id, date_start, date_end, siteUrl)
            self.df_search_console_page_query = self.save_to_page_query_table(web_id, date_start, date_end, siteUrl)
            self.df_search_console_page = self.save_to_page_table(web_id, date_start, date_end, siteUrl)
            self.df_search_console_device = self.save_to_device_table(web_id, date_start, date_end, siteUrl)

    ## save to query table and
    def save_to_page_query_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000):
        df_search_console_page_query = self.fetch_search_console(web_id, date_start, date_end, siteUrl,
                                                     rowLimit=rowLimit, dimensions=['query', 'page', 'date'])
        df_search_console_page_query = self.reformat_title_from_url(df_search_console_page_query, is_page_query=True)

        search_console_query_list_dict = df_search_console_page_query.to_dict('records')
        query_pg = MySqlHelper.generate_update_SQLquery(df_search_console_page_query, 'google_search_console_page_query')
        MySqlHelper('roas_report').ExecuteUpdate(query_pg, search_console_query_list_dict)

        return df_search_console_page_query

    ## save to query table and
    def save_to_query_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000):
        df_search_console_query = self.fetch_search_console(web_id, date_start, date_end, siteUrl,
                                                     rowLimit=rowLimit, dimensions=['query', 'date', 'country', 'device'])
        search_console_query_list_dict = df_search_console_query.to_dict('records')
        query_q = "REPLACE INTO google_search_console_query (web_id, clicks, impressions, position, query, device, country, date) VALUES (:web_id, :clicks, :impressions, :position, :query, :device, :country, :date)"
        print(query_q)
        MySqlHelper('roas_report').ExecuteUpdate(query_q, search_console_query_list_dict)

        return df_search_console_query

    def save_to_page_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000):
        df_search_console_page = self.fetch_search_console(web_id, date_start, date_end, siteUrl, rowLimit=rowLimit, dimensions=['page', 'date'])
        df_search_console_page = self.reformat_title_from_url(df_search_console_page, is_page_query=False)

        search_console_page_list_dict = df_search_console_page.to_dict('records')
        query = "REPLACE INTO google_search_console_page (web_id, clicks, impressions, position, page, date) VALUES (:web_id, :clicks, :impressions, :position, :page, :date)"
        print(query)
        MySqlHelper('roas_report').ExecuteUpdate(query, search_console_page_list_dict)
        return df_search_console_page

    def save_to_device_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000):
        df_search_console_device = self.fetch_search_console(web_id, date_start, date_end, siteUrl, rowLimit=rowLimit, dimensions=['device', 'date'])
        search_console_device_list_dict = df_search_console_device.to_dict('records')
        query = "REPLACE INTO google_search_console_device (web_id, clicks, impressions, position, device, date) VALUES (:web_id, :clicks, :impressions, :position, :device, :date)"
        print(query)
        MySqlHelper('roas_report').ExecuteUpdate(query, search_console_device_list_dict)
        return df_search_console_device

    def fetch_search_console(self, web_id, date_start, date_end, siteUrl, dimensions=['query', 'date', 'country', 'device'],
                             rowLimit=25000):
        ## set up Google Oauth 2.0
        service = self.service
        ## payload for fetch data
        request = {
            'startDate': date_start,
            'endDate': date_end,
            'dimensions': dimensions,  ## country, device, page, query, searchAppearance
            'rowLimit': rowLimit
        }
        response = service.searchanalytics().query(
            siteUrl=siteUrl, body=request).execute()
        data = response['rows']
        data_decompose = {}
        for i, d in enumerate(data):
            # query, device, date, url_page = d['keys']
            # data_decompose[i] = {'web_id': web_id, 'query': query, 'device': device, 'date': date, 'url_page': url_page,
            #                      'clicks': d['clicks'], 'impressions': d['impressions'], 'position': d['position']}
            data_decompose[i] = {'web_id': web_id, 'clicks': d['clicks'],
                                 'impressions': d['impressions'], 'position': d['position']}
            for j,dim in enumerate(dimensions):
                data_decompose[i][dim] = d['keys'][j]
        df_search_console = pd.DataFrame.from_dict(data_decompose, "index")
        # search_console_list_dict = df_search_console.to_dict('records')
        return df_search_console

    ## adding product_id, title, description by url
    def reformat_title_from_url(self, df_search_console_page, is_page_query=True):
        url_list = list(set(df_search_console_page['page']))
        url_clean_list = list(set([url.split('?')[0] for url in url_list]))
        web_id = df_search_console_page['web_id'][0]
        url_dict = self.fetch_title_by_url(web_id, url_clean_list)
        data_dict = {}
        for index, row in df_search_console_page.iterrows():
            if is_page_query: ## page table and page_query table
                web_id, clicks, impressions, position, query, page, date = row
            else:
                web_id, clicks, impressions, position, page, date = row
            if page in url_dict.keys():
                product_id, title, description = url_dict[page]
            else:  # not in
                product_id, title, description = '_', '_', '_'
            if is_page_query:  ## page table and page_query table
                data_dict[index] = {'web_id': web_id, 'product_id': product_id, 'title': title, 'description': description,
                                    'clicks': clicks, 'impressions': impressions, 'position': position, 'page': page,
                                    'query':query, 'date': date}
            else:
                data_dict[index] = {'web_id': web_id, 'product_id': product_id, 'title': title, 'description': description,
                                    'clicks': clicks, 'impressions': impressions, 'position': position, 'page': page,
                                    'date': date}
        df_reformat = pd.DataFrame.from_dict(data_dict, "index")
        return df_reformat

    def fetch_title_by_url(self, web_id, url_list, ):
        query = f"SELECT url, product_id, title, description FROM report_data.item_list WHERE web_id='{web_id}' and url in ("
        for i, url in enumerate(url_list):
            query += f"'{url}', "
            if i == len(url_list) - 1:
                query += f"'{url}')"
        print(query)
        data = MySqlHelper('report_data_webpush-api02').ExecuteSelect(query)
        dict_map = {}
        for d in data:
            dict_map[d[0]] = d[1:]
        return dict_map


    def _generate_keyword_metrics(self, keyword_list, path_ads_config):
        n_keyword = len(keyword_list)
        indexes = np.append(np.arange(0, n_keyword, 20), n_keyword)
        google_ad = GoogleAds(path_ads_config=path_ads_config)
        df_keywords_metrics = pd.DataFrame()
        for i in range(len(indexes)-1):
            keyword_list_sub = keyword_list[indexes[i]: indexes[i+1]]
            print(f"saving query table..., keyword size is {n_keyword}, select index between {indexes[i]} and {indexes[i+1]}")
            df_keywords_metrics = df_keywords_metrics.append(google_ad.get_keyword_list_info(keyword_list_sub))
        return df_keywords_metrics

    def _generate_12month_keyword_metrics(self, keyword_list, path_ads_config):
        n_keyword = len(keyword_list)
        indexes = np.append(np.arange(0, n_keyword, 20), n_keyword)
        google_ad = GoogleAds(path_ads_config=path_ads_config)
        df_keywords_metrics = pd.DataFrame()
        for i in range(len(indexes)-1):
            keyword_list_sub = keyword_list[indexes[i]: indexes[i+1]]
            print(f"saving query table..., keyword size is {n_keyword}, select index between {indexes[i]} and {indexes[i+1]}")
            df_keywords_metrics = df_keywords_metrics.append(google_ad.get_keyword_list_monthly_info(keyword_list_sub))
        return df_keywords_metrics

    def groupby_dim(self, df, dim=['']):
        df_group = df.groupby(dim).sum()
        clicks = sum(df_group['clicks'])
        impressions = sum(df_group['impressions'])
        return clicks, impressions, df_group



if __name__ == '__main__':
    ## setup parameters
    # site = 'https://www.avividai.com'
    # site = 'https://www.ehaostore.com/' ## web_id = 'ehaostore'
    # property_uri = 'https://www.ehaostore.com/' ## web_id = 'ehaostore'
    # property_uri = 'https://i3fresh.tw/' ## web_id = 'i3fresh'
    ## set up parameters
    # siteUrl = 'https://www.nanooneshop.com/' ## web_id = 'nanooneshop'
    # web_id = 'nanooneshop'
    # web_id = 'i3fresh'
    # siteUrl = 'https://i3fresh.tw/'
    # date_start = '2021-11-10'
    # date_end = '2021-11-10'

    ############### init db ###############
    web_id = 'i3fresh'
    siteUrl = 'https://i3fresh.tw/'
    date_start = '2021-11-09'
    date_end = '2021-11-09'
    g_search = GoogleSearchConsole()
    g_search.save_4db_by_date(web_id, siteUrl, date_start, date_end)
    ############### init db ###############


