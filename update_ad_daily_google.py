from predict_hot_item.Ecom import Ecom
from basic.date import get_yesterday, check_is_UTC0, to_datetime, date_range
from db.mysqlhelper import MySqlHelper


def update_ad_daily_google(web_id, date):
    df = Ecom().fetch_daily_report(web_id, date, date, use_daily=False)
    if df.shape[0]==0:
        print('data is empty')
    else:
        data_list_dict = df.to_dict('records')
        query = MySqlHelper.generate_update_SQLquery(df, 'cdp_ad_daily_google')
        MySqlHelper('cdp').ExecuteUpdate(query, data_list_dict)
    return df


if __name__ == '__main__':

    is_UTC0 = check_is_UTC0()
    ecom = Ecom()
    date_start = get_yesterday(is_UTC0=is_UTC0)
    # date_start = to_datetime('2021-11-20')
    dateRange = date_range(date_start, 1)
    web_id_all = ecom.fetch_all_web_id()
    # web_id_all = ['i3fresh']
    for date in dateRange:
        for web_id in web_id_all:
            print(f'insert {web_id}')
            ## only update yesterday data (collection is finished)
            df = update_ad_daily_google(web_id, date)

            # df = ecom.fetch_daily_report(web_id, date_start, date_start, use_daily=False)
            # data_list_dict = df.to_dict('records')
            # query = MySqlHelper.generate_update_SQLquery(df, 'cdp_ad_daily_google')
            # MySqlHelper('cdp').ExecuteUpdate(query, data_list_dict)

