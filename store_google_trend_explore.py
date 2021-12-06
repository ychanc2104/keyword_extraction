from gAPI.gtrend import GoogleTrend
from basic.date import get_today, get_date_shift, check_is_UTC0, datetime_to_str
from db.mysqlhelper import MySqlHelper

def update_google_explore(keyword='', property='', timeframe=None):
    """
    Parameters
    ----------
    keyword: '' for popular queries
    property: ''(web), 'images', 'news', 'youtube', 'froogle'
    timeframe: '2021-10-22 2021-10-22'

    Returns
    -------
    """
    gtrend = GoogleTrend()
    if timeframe==None:
        date_end = get_today(check_is_UTC0())
        date_start = get_date_shift(date_end, 6)
        date_start, date_end = datetime_to_str(date_start), datetime_to_str(date_end)
        timeframe = date_start + ' ' + date_end
    response_all = gtrend.fetch_keyword_expore(keyword=keyword, gprop=property,timeframe=timeframe)
    df_topic_hot, df_topic_up = response_all['SearchTopic']['hot'], response_all['SearchTopic']['up']
    df_query_hot, df_query_up = response_all['SearchQuery']['hot'], response_all['SearchQuery']['up']
    df_query_hot['property'], df_query_up['property'] = df_query_hot.shape[0] * [property], df_query_up.shape[0] * [property]
    df_query_hot['type'], df_query_up['type'] = df_query_hot.shape[0] * ['hot'], df_query_hot.shape[0] * ['up']
    df_query_hot['date_start'], df_query_hot['date_end'] = df_query_hot.shape[0] * [date_start], df_query_hot.shape[0] * [date_end]
    df_query_up['date_start'], df_query_up['date_end'] = df_query_up.shape[0] * [date_start], df_query_up.shape[0] * [date_end]
    df_query_hot = df_query_hot.drop(columns=['formattedValue', 'hasData', 'link'])
    df_query_up = df_query_up.drop(columns=['link'])
    query_hot = MySqlHelper.generate_update_SQLquery(df_query_hot, 'google_trend_explore')
    query_up = MySqlHelper.generate_update_SQLquery(df_query_up, 'google_trend_explore')
    MySqlHelper('dione').ExecuteUpdate(query_hot, df_query_hot.to_dict('records'))
    MySqlHelper('dione').ExecuteUpdate(query_up, df_query_up.to_dict('records'))
    return df_query_hot, df_query_up

if __name__ == '__main__':
    gtrend = GoogleTrend()
    ## save 7-day popular google trend explore keywords
    df_query_hot, df_query_up = update_google_explore(property='youtube')
