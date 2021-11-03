from predict_hot_item.Ecom import Ecom



if __name__ == '__main__':
    # web_id = 'i3fresh'
    # date_start = '2021-10-10'
    ecom = Ecom()
    date_yesterday = ecom._get_yesterday()
    web_id_all = ecom.fetch_all_web_id(date_start=date_yesterday)
    for web_id in web_id_all:
        print(f'insert {web_id}')
        # df = ecom.sqldate(web_id, date_start, date_yesterday)
        ## only update yesterday data (collection is finished)
        ecom.insert_daily_report(web_id, mode_insert='yesterday')
