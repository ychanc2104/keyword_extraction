from gAPI.gtrend import gtrend
from db.mysqlhelper import MySqlHelper
from gAPI.gconsole import GoogleSearchConsole
from basic.date import get_today, get_date_shift, check_is_UTC0, datetime_to_str
from basic.decorator import timing

## generate keyword metrics and save to normal and historical table
def save_keyword_metrics(keyword_list):
    is_UTC0 = check_is_UTC0()
    today = get_today(is_UTC0=is_UTC0)
    year, month = today.year, today.month

    # df_keyword_metrics = GoogleSearchConsole()._generate_keyword_metrics(keyword_list, path_ads_config='./gAPI/google-ads.yaml')
    df_monthly_keyword_metrics = GoogleSearchConsole()._generate_12month_keyword_metrics(keyword_list, path_ads_config='./gAPI/google-ads.yaml')
    df_monthly_keyword_metrics = df_monthly_keyword_metrics.query(f"year=='{year}' and month=='{month-1}'")
    df_keyword_metrics = df_monthly_keyword_metrics.query(f"year=='{year}' and month=='{month}'").drop(columns=['monthly_traffic', 'year', 'month'])
    df_keyword_metrics['date'] = [datetime_to_str(today)] * df_keyword_metrics.shape[0]
    keyword_metrics_list_dict = df_keyword_metrics.to_dict('records')
    monthly_keyword_metrics_list_dict = df_monthly_keyword_metrics.to_dict('records')

    ## generate sql script by DataFrame
    # query = MySqlHelper.generate_update_SQLquery(df_keyword_metrics, 'google_ads_metrics')
    query_history = MySqlHelper.generate_update_SQLquery(df_monthly_keyword_metrics, 'google_ads_metrics_history')
    ## save to metrics tables
    # MySqlHelper('roas_report').ExecuteUpdate(query, keyword_metrics_list_dict)
    MySqlHelper('roas_report').ExecuteUpdate(query_history, monthly_keyword_metrics_list_dict)
    return df_keyword_metrics, df_monthly_keyword_metrics


## fetch keywords yesterday + today
@timing
def fetch_new_gtrend_keywords():
    # if date_start==None:
    #     date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    # query = f"SELECT keyword FROM google_trend_keyword where add_time>'{date_start}'"
    query = f"""
            SELECT DISTINCT
                keyword AS keyword
            FROM
                dione.google_trend_keyword
            WHERE
                keyword NOT IN                 
                (SELECT 
                    keyword_ask
                FROM
                    roas_report.google_ads_metrics_history)
            """

    print(query)
    data = MySqlHelper("dione").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list

## fetch keywords yesterday + today
@timing
def fetch_new_gconsole_keywords():
    # if date_start==None:
    #     date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    # query = f"SELECT query FROM google_search_console_query where add_time>'{date_start}'"
    query = f"""
            SELECT DISTINCT
                query AS query
            FROM
                google_search_console_query
            WHERE
                query NOT IN 
                (SELECT 
                    keyword_ask
                FROM
                    google_ads_metrics_history)
            """

    print(query)
    data = MySqlHelper("roas_report").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list


def save_init_monthly_keyword_metrics():
    date_start = '2021-01-01'
    is_UTC0 = check_is_UTC0()
    keyword_list_gtrend = fetch_new_gconsole_keywords()
    keyword_list_gconsole = fetch_new_gconsole_keywords()
    keyword_list = list(set(keyword_list_gtrend + keyword_list_gconsole))

    df_monthly_keyword_metrics = GoogleSearchConsole()._generate_12month_keyword_metrics(keyword_list, path_ads_config='./gAPI/google-ads.yaml')
    monthly_keyword_metrics_list_dict = df_monthly_keyword_metrics.to_dict('records')
    # df_keyword_metrics = df_monthly_keyword_metrics.drop(columns=['monthly_traffic', 'year', 'month'])
    # keyword_metrics_list_dict = df_keyword_metrics.to_dict('records')
    ## generate sql script by DataFrame
    # query = MySqlHelper.generate_update_SQLquery(df_keyword_metrics, 'google_ads_metrics')
    query_history = MySqlHelper.generate_update_SQLquery(df_monthly_keyword_metrics, 'google_ads_metrics_history')
    ## save to metrics tables
    # MySqlHelper('roas_report').ExecuteUpdate(query, keyword_metrics_list_dict)
    MySqlHelper('roas_report').ExecuteUpdate(query_history, monthly_keyword_metrics_list_dict)
    return df_monthly_keyword_metrics

## update rate: twice per day (13:30, 23:30)
if __name__ == '__main__':
    ## setting parameters
    # is_UTC0 = check_is_UTC0()
    # date_start = get_date_shift(get_today(is_UTC0=is_UTC0), days=1)
    # ###################################
    # # ## for google trend table
    # keyword_list_gtrend = fetch_new_gtrend_keywords()
    # # # df_gtrend_keyword_metrics, df_gtrend_monthly_keyword_metrics = save_keyword_metrics(keyword_list_gtrend)
    # # ###################################
    # # ## for google search_console table
    # keyword_list_gconsole = fetch_new_gconsole_keywords()
    # # df_gconsole_keyword_metrics, df_gconsole_monthly_keyword_metrics = save_keyword_metrics(keyword_list_gconsole)

    ################ init table, google_ads_metrics_history ###################
    df = save_init_monthly_keyword_metrics()
    ################ init table, google_ads_metrics_history ###################
    # gg = GoogleSearchConsole()
    # df_monthly_keyword_metrics = gg._generate_12month_keyword_metrics(['CPTPP'], path_ads_config='./gAPI/google-ads.yaml')

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


