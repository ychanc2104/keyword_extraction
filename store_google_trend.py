from gAPI.gtrend import GoogleTrend
from db.mysqlhelper import MySqlHelper




if __name__ == '__main__':
    # df_30day = gtrend().insert_gtrend_keywords(filter_repeat=True)
    df_2day = GoogleTrend().update_latest_two_days()
    # df_30day_list_of_dict = df_30day.to_dict('records')
    #
    # MySqlHelper('dione').ExecuteInsert('google_trend_keyword', df_30day_list_of_dict)



