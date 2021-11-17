import os
import pandas as pd
import datetime
import numpy as np
from gAPI.googleoauth2 import GoogleOAuth2
from gAPI.gads import GoogleAds
from db.mysqlhelper import MySqlHelper
from basic.date import to_datetime, get_date_shift, get_today, datetime_to_str


class GoogleSearchConsole(GoogleOAuth2):

    def update_3db(self, web_id, siteUrl, path_ads_config='google-ads.yaml'):
        date_start = datetime_to_str(get_date_shift(days=3))
        date_end = datetime_to_str(get_today())
        print(f"update from {date_start} to {date_end}")
        self.save_to_query_table(web_id, date_start, date_end, siteUrl, path_ads_config=path_ads_config)
        self.save_to_page_table(web_id, date_start, date_end, siteUrl)
        self.save_to_device_table(web_id, date_start, date_end, siteUrl)

    def save_4db_by_date(self, web_id, siteUrl, date_start='2021-01-01', path_ads_config='google-ads.yaml'):
        # date_start = '2021-01-01'
        date_end = datetime_to_str(get_today())
        num_days = (to_datetime(date_end) - to_datetime(date_start)).days
        num_days_list = [30*(i+1) for i in range(int(num_days/30))] + [num_days]
        date_range_list = [to_datetime(date_start) + datetime.timedelta(days=x) for x in num_days_list]
        for i in range(len(date_range_list)-1):
            date_start = datetime_to_str(date_range_list[i])
            date_end = datetime_to_str(date_range_list[i+1])
            print(f"update date range from {date_start} to {date_end}...")
            self.save_to_query_table(web_id, date_start, date_end, siteUrl, path_ads_config=path_ads_config)
            self.save_to_page_table(web_id, date_start, date_end, siteUrl)
            self.save_to_device_table(web_id, date_start, date_end, siteUrl)


    ## save to query table and
    def save_to_query_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000, path_ads_config='google-ads.yaml'):
        df_search_console_query = self.fetch_search_console(web_id, date_start, date_end, siteUrl,
                                                     rowLimit=rowLimit, dimensions=['query', 'date', 'country', 'device'])
        search_console_query_list_dict = df_search_console_query.to_dict('records')
        query_q = "REPLACE INTO google_search_console_query (web_id, clicks, impressions, position, query, device, country, date) VALUES (:web_id, :clicks, :impressions, :position, :query, :device, :country, :date)"
        print(query_q)
        MySqlHelper('roas_report').ExecuteUpdate(query_q, search_console_query_list_dict)

        # keyword_list = list(set(df_search_console_query['query']))
        # df_keywords_metrics = self._generate_keyword_metrics(keyword_list, path_ads_config)
        # keywords_metrics_list_dict = df_keywords_metrics.to_dict('records')
        # query_ads = "REPLACE INTO google_ads_metrics " \
        #             "(keyword_join, keyword_ask, keyword_google, low_price, high_price, avg_monthly_traffic, " \
        #             "competition_level, competition_value, date) VALUES " \
        #             "(:keyword_join, :keyword_ask, :keyword_google, :low_price, :high_price, :avg_monthly_traffic, " \
        #             ":competition_level, :competition_value, :date)"
        # print(query_ads)
        # MySqlHelper('roas_report').ExecuteUpdate(query_ads, keywords_metrics_list_dict)

        return df_search_console_query

    def save_to_page_table(self, web_id, date_start, date_end, siteUrl, rowLimit=25000):
        df_search_console_page = self.fetch_search_console(web_id, date_start, date_end, siteUrl, rowLimit=rowLimit, dimensions=['page', 'date'])
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
    # date_start = '2021-11-05'
    # date_end = '2021-11-10'
    #
    # SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
    # CLIENT_SECRETS_PATH = 'client_secrets.json' ## use likrrobot@avividai.com
    # CLIENT_SECRETS_PATH = os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_PATH)
    #
    # g_search = GoogleSearchConsole()
    # g_search.update_4db(web_id, siteUrl)

    keyword_list = ['iphone', '空壓殼']
    # df_monthly_keyword_metrics = GoogleSearchConsole()._generate_keyword_metrics(keyword_list,
    #                                                                              path_ads_config='../gAPI/google-ads.yaml')
    #
    # df2 = GoogleAds(path_ads_config='../gAPI/google-ads.yaml').get_keyword_list_info(keyword_list)

    df_monthly_keyword_metrics = GoogleSearchConsole()._generate_12month_keyword_metrics(keyword_list,
                                                                                 path_ads_config='../gAPI/google-ads.yaml')

    # query = "SELECT web_id FROM dione.web_id_table where missoner_keyword_enable=1"
    # print(query)
    # data = MySqlHelper('RDS').ExecuteSelect(query)

    # df_search_console_query, df_keywords_metrics = g_search.save_to_query_ads_table(web_id, date_start, date_end, siteUrl)
    # df_search_console_page = g_search.save_to_page_table(web_id, date_start, date_end, siteUrl)
    # df_search_console_device = g_search.save_to_device_table(web_id, date_start, date_end, siteUrl)


    # df_search_console = g_search.fetch_search_console(web_id=web_id, date_start=date_start, date_end=date_end,
    #                                                   siteUrl=siteUrl, dimensions=['query', 'date', 'country', 'device'])
    #
    # df_test = g_search.fetch_search_console(web_id=web_id, date_start=date_start, date_end=date_end,
    #                                                   siteUrl=siteUrl, dimensions=['device', 'date'])
    #
    # # df_search_console_q = g_search.fetch_search_console(web_id=web_id, date_start=date_start, date_end=date_end,
    # #                                                   siteUrl=siteUrl, dimensions=['query'])
    # #
    # # df_search_console_page = g_search.fetch_search_console(web_id=web_id, date_start=date_start, date_end=date_end,
    # #                                                   siteUrl=siteUrl, dimensions=['page', 'date'])
    #
    # clicks_query, impressions_query, df_group_query = g_search.groupby_dim(df_search_console, dim=['date'])
    # clicks_page, impressions_page, df_group_page = g_search.groupby_dim(df_search_console_page, dim=['page'])
    # clicks_device, impressions_device, df_group_device = g_search.groupby_dim(df_search_console_device, dim=['device'])

    # clicks, impressions, df_group = g_search.groupby_dim(df_test, dim=['device'])

    # g_search.save_to_db(df_search_console)

    # g_search.init_save_db(web_id, siteUrl, date_start='2021-01-01')
    # df_date = df_search_console[['clicks', 'impressions', 'date']].groupby(['date']).sum()
    # df_search_console_q2 = df_search_console.groupby(['query']).sum()
    # df_search_console_c = df_search_console.groupby(['country']).sum()
    #
    #
    # df_search_console_s = df_search_console.query("date=='2021-10-01'")
    # # df_search_page = df_search_console.query("page=='https://www.nanooneshop.com/'")
    #
    # impressions = sum(df_search_console_s['impressions'])
    # impression_all = sum(df_search_console['impressions'])

    # ## set up Google Oauth 2.0
    # Gauth2 = GoogleOauth2()
    # service = Gauth2.service
    # ## payload for fetch data
    # request = {
    #     'startDate': date_start,
    #     'endDate': date_end,
    #     'dimensions': ['query','device','date','page'], ## country, device, page, query, searchAppearance
    #     'rowLimit': 25000
    # }
    # response = service.searchanalytics().query(
    #     siteUrl=siteUrl, body=request).execute()
    #
    # data = response['rows']
    # data_decompose = {}
    # for i,d in enumerate(data):
    #     query, device, date, url_page = d['keys']
    #     data_decompose[i] = {'query': web_id, 'query': query, 'device': device, 'date': date, 'url_page': url_page,
    #                          'clicks': d['clicks'], 'impressions': d['impressions'], 'position': d['position']}
    # df_search_console = pd.DataFrame.from_dict(data_decompose, "index")
    # search_console_list_dict = df_search_console.to_dict('records')



    # print('Auth Successful')
    ## {"access_token": "ya29.a0ARrdaM-JXs8gAIprCrZs6dy_FCbfww7b8cJPmEqirYKhoszkpGoepKNUjRuss-llmec813pQqhv9G1_lf5iEJnhiOBdxH9sDP1Hq9M7jv3gsUao1uzN953qmBkB96qEN4rjysT3JXp75oDwwgS8PhLIsmx9uzw", "client_id": "79879131208-gonrnvu7qqe329hu4oifd8l2lmc30gkb.apps.googleusercontent.com", "client_secret": "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", "refresh_token": "1//0eU5bD-84aW2ECgYIARAAGA4SNwF-L9Ir5G7b3Rlv6YeprCBEckbCgZP02mT_g6HOSMBxg26wau4mADujssqBfHQ1ea-Y61dMJFM", "token_expiry": "2021-11-08T05:37:21Z", "token_uri": "https://oauth2.googleapis.com/token", "user_agent": null, "revoke_uri": "https://oauth2.googleapis.com/revoke", "id_token": null, "id_token_jwt": null, "token_response": {"access_token": "ya29.a0ARrdaM-JXs8gAIprCrZs6dy_FCbfww7b8cJPmEqirYKhoszkpGoepKNUjRuss-llmec813pQqhv9G1_lf5iEJnhiOBdxH9sDP1Hq9M7jv3gsUao1uzN953qmBkB96qEN4rjysT3JXp75oDwwgS8PhLIsmx9uzw", "expires_in": 3599, "scope": "https://www.googleapis.com/auth/webmasters.readonly", "token_type": "Bearer"}, "scopes": ["https://www.googleapis.com/auth/webmasters.readonly"], "token_info_uri": "https://oauth2.googleapis.com/tokeninfo", "invalid": false, "_class": "OAuth2Credentials", "_module": "oauth2client.client"}


