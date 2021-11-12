from gAPI.gtrend import gtrend
from db.mysqlhelper import MySqlHelper
from gAPI.gconsole import GoogleSearchConsole
from basic.date import get_today, get_date_shift, check_is_UTC0

## generate keyword metrics and save to normal and historical table
def save_keyword_metrics(keyword_list):
    df_keyword_metrics = GoogleSearchConsole()._generate_keyword_metrics(keyword_list, path_ads_config='./gAPI/google-ads.yaml')
    keyword_metrics_list_dict = df_keyword_metrics.to_dict('records')
    ## generate sql script by DataFrame
    query = MySqlHelper.generate_update_SQLquery(df_keyword_metrics, 'google_ads_metrics')
    query_history = MySqlHelper.generate_update_SQLquery(df_keyword_metrics, 'google_ads_metrics_history')
    ## save to metrics tables
    MySqlHelper('roas_report').ExecuteUpdate(query, keyword_metrics_list_dict)
    MySqlHelper('roas_report').ExecuteUpdate(query_history, keyword_metrics_list_dict)


## fetch keywords yesterday + today
def fetch_gtrend_keywords(date_start=None, is_UTC0=True):
    if date_start==None:
        date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    query = f"SELECT keyword FROM google_trend_keyword where add_time>'{date_start}'"
    print(query)
    data = MySqlHelper("dione").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list

## fetch keywords yesterday + today
def fetch_gconsole_keywords(date_start=None, is_UTC0=True):
    if date_start==None:
        date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    query = f"SELECT query FROM google_search_console_query where add_time>'{date_start}'"
    print(query)
    data = MySqlHelper("roas_report").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list


## update rate: twice per day (13:30, 23:30)
if __name__ == '__main__':
    ## setting parameters
    is_UTC0 = check_is_UTC0()
    date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    ###################################
    # ## for google trend table
    keyword_list_gtrend = fetch_gtrend_keywords(date_start=date_start, is_UTC0=is_UTC0)
    save_keyword_metrics(keyword_list_gtrend)
    ###################################
    ## for google search_console table
    keyword_list_gconsole = fetch_gconsole_keywords(date_start=date_start, is_UTC0=is_UTC0)
    save_keyword_metrics(keyword_list_gconsole)


    ###################################
    # ## for google trend table
    # keyword_list_gtrend = fetch_gtrend_keywords(date_start=date_start, is_UTC0=is_UTC0)
    # df_keyword_gtrend_metrics = g_search._generate_keyword_metrics(keyword_list_gtrend, path_ads_config='./gAPI/google-ads.yaml')
    # keyword_gtrend_metrics_list_dict = df_keyword_gtrend_metrics.to_dict('records')
    # ## generate sql script by DataFrame
    # query = MySqlHelper.generate_update_SQLquery(df_keyword_gtrend_metrics, 'google_ads_metrics')
    # query_history = MySqlHelper.generate_update_SQLquery(df_keyword_gtrend_metrics, 'google_ads_metrics_history')
    # ## save to tables
    # # MySqlHelper('roas_report').ExecuteUpdate(query, keyword_gtrend_metrics_list_dict)
    # MySqlHelper('roas_report').ExecuteUpdate(query_history, keyword_gtrend_metrics_list_dict)

    ###################################
    ## for google search_console table
    # keyword_list_gconsole = fetch_gconsole_keywords(date_start=date_start, is_UTC0=is_UTC0)
    # df_keyword_gconsole_metrics = g_search._generate_keyword_metrics(keyword_list_gconsole, path_ads_config='./gAPI/google-ads.yaml')
    # keyword_gconsole_metrics_list_dict = df_keyword_gconsole_metrics.to_dict('records')
    # ## generate sql script by DataFrame
    # query2 = MySqlHelper.generate_update_SQLquery(df_keyword_gconsole_metrics, 'google_ads_metrics')
    # query2_history = MySqlHelper.generate_update_SQLquery(df_keyword_gconsole_metrics, 'google_ads_metrics_history')
    # ## save to tables
    # # MySqlHelper('roas_report').ExecuteUpdate(query, keyword_gconsole_metrics_list_dict)
    # MySqlHelper('roas_report').ExecuteUpdate(query2_history, keyword_gconsole_metrics_list_dict)


