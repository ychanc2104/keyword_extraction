import pandas as pd
import datetime
from gAPI.googleoauth2 import GoogleOAuth2
from gAPI import GoogleAds
from db import DBhelper
from basic import to_datetime, get_date_shift, datetime_to_str, curdate

# check Method Resolution Order, MRO, GoogleSearchConsole.__mro__
class GoogleSearchConsole(GoogleOAuth2, GoogleAds):
    def __init__(self):
        ## GoogleSearchConsole.__mro__
        super().__init__()

    ## update 4db after initalize table
    def update_4db(self, web_id, siteUrl, rowLimit=25000, save=True):
        date_end = datetime_to_str(curdate(utc=8))
        date_start = datetime_to_str(get_date_shift(date_ref=date_end, days=3))
        print(f"update from {date_start} to {date_end}")
        df_query = self.save_to_query_table(web_id, date_start, date_end, siteUrl, rowLimit, save)
        df_page_query = self.save_to_page_query_table(web_id, date_start, date_end, siteUrl, rowLimit, save)
        df_page = self.save_to_page_table(web_id, date_start, date_end, siteUrl, rowLimit, save)
        df_device = self.save_to_device_table(web_id, date_start, date_end, siteUrl, rowLimit, save)
        return df_query, df_page_query, df_page, df_device
    ## for init table, fetch and save data day by day
    def save_4db_by_date(self, web_id, siteUrl, date_start='2021-01-01', date_end=None):
        if date_end==None:
            date_end = datetime_to_str(curdate())
        num_days = (to_datetime(date_end) - to_datetime(date_start)).days + 1
        date_list = [to_datetime(date_start) + datetime.timedelta(days=x) for x in range(num_days)]
        if num_days==1:
            date_list += date_list
        for i in range(len(date_list)-1):
            date_start = datetime_to_str(date_list[i])
            date_end = datetime_to_str(date_list[i+1])
            print(f"update date range from {date_start} to {date_end}...")
            df_query = self.save_to_query_table(web_id, date_start, date_end, siteUrl)
            df_page_query = self.save_to_page_query_table(web_id, date_start, date_end, siteUrl)
            df_page = self.save_to_page_table(web_id, date_start, date_end, siteUrl)
            df_device = self.save_to_device_table(web_id, date_start, date_end, siteUrl)
        return df_query, df_page_query, df_page, df_device
    ## save to query table and
    def save_to_page_query_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000, save=True):
        df_page_query = self.fetch_search_console(web_id, date_start, date_end, siteUrl,
                                                     rowLimit=rowLimit, dimensions=['query', 'page', 'date'])
        if df_page_query.shape[0] != 0:
            df_page_query = self.reformat_title_from_url(df_page_query, is_page_query=True)
        if save:
            query = DBhelper.generate_insertDup_SQLquery(df_page_query,
                                                         'google_search_console_page_query',
                                                         df_page_query.columns)
            DBhelper('roas_report').ExecuteUpdate(query, df_page_query.to_dict('records'))
        return df_page_query

    ## save to query table and
    def save_to_query_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000, save=True):
        df_query = self.fetch_search_console(web_id, date_start, date_end, siteUrl,
                                                     rowLimit=rowLimit, dimensions=['query', 'date', 'country', 'device'])
        if save:
            query = DBhelper.generate_insertDup_SQLquery(df_query,
                                                         'google_search_console_query',
                                                         df_query.columns)
            DBhelper('roas_report').ExecuteUpdate(query, df_query.to_dict('records'))
        return df_query

    def save_to_page_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000, save=True):
        df_page = self.fetch_search_console(web_id, date_start, date_end, siteUrl, rowLimit=rowLimit, dimensions=['page', 'date'])
        if df_page.shape[0] != 0:
            df_page = self.reformat_title_from_url(df_page, is_page_query=False)
        if save:
            query = DBhelper.generate_insertDup_SQLquery(df_page,
                                                         'google_search_console_page',
                                                         df_page.columns)
            DBhelper('roas_report').ExecuteUpdate(query, df_page.to_dict('records'))
        return df_page

    def save_to_device_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000, save=True):
        df_device = self.fetch_search_console(web_id, date_start, date_end, siteUrl, rowLimit=rowLimit, dimensions=['device', 'date'])
        if save:
            query = DBhelper.generate_insertDup_SQLquery(df_device,
                                                         'google_search_console_device',
                                                         df_device.columns)
            DBhelper('roas_report').ExecuteUpdate(query, df_device.to_dict('records'))
        return df_device

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
        response = service.searchanalytics().query(siteUrl=siteUrl, body=request).execute()
        self.response = response
        data = response.get('rows', [])
        data_decompose = {}
        for i, d in enumerate(data):
            data_decompose[i] = {'web_id': web_id, 'clicks': d['clicks'],
                                 'impressions': d['impressions'], 'position': d['position']}
            for j,dim in enumerate(dimensions):
                data_decompose[i][dim] = d['keys'][j]
        df_search_console = pd.DataFrame.from_dict(data_decompose, "index")
        return df_search_console

    ## adding product_id, title, description by url
    @classmethod
    def reformat_title_from_url(cls, df_search_console_page, is_page_query=True):
        url_list = list(set(df_search_console_page['page']))
        url_clean_list = list(set([url.split('?')[0] for url in url_list]))
        web_id = df_search_console_page['web_id'][0]
        url_dict = cls.fetch_title_by_url(web_id, url_clean_list)
        data_dict = {}
        for index, row in df_search_console_page.iterrows():
            if is_page_query: ## page table and page_query table
                web_id, clicks, impressions, position, query, page, date = row
            else:
                web_id, clicks, impressions, position, page, date = row
            page = page.lower()
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

    @staticmethod
    def fetch_title_by_url(web_id, url_list, ):
        query = f"SELECT url, product_id, title, description FROM report_data.item_list WHERE web_id='{web_id}' and url in ("
        for i, url in enumerate(url_list):
            url = url.lower()
            query += f"'{url}', "
            if i == len(url_list) - 1:
                query += f"'{url}')"
        print(query)
        data = DBhelper('report_data_webpush-api02').ExecuteSelect(query)
        dict_map = {}
        for d in data:
            dict_map[d[0]] = d[1:]
        return dict_map



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
    web_id = 'draimior'
    siteUrl = 'https://www.draimior-global.com/'
    date_start = '2022-06-01' ## '2021-09-06'
    date_end = '2022-06-20' ## '2021-09-30'
    g_search = GoogleSearchConsole()
    # df_search_console_query = g_search.fetch_search_console(web_id, date_start, date_end, siteUrl,
    #                                                  rowLimit=25000, dimensions=['query', 'date', 'country', 'device'])
    # g_search.save_4db_by_date(web_id, siteUrl, date_start, date_end)

    ############### update db ###############
    df_list = g_search.update_4db(web_id, siteUrl, save=False)


