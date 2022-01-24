from gAPI.gtrend import GoogleTrend
from db import MySqlHelper




if __name__ == '__main__':
    # df_30day = gtrend().insert_gtrend_keywords(filter_repeat=True)
    df_2day = GoogleTrend().update_latest_two_days()



