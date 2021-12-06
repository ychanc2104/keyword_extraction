from gAPI.gtrend import GoogleTrend


if __name__ == '__main__':
    gtrend = GoogleTrend()
    ## save daily popular google trend keywords
    df_2day = gtrend.update_latest_two_days()



