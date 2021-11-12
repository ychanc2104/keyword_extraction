import socket
from predict_hot_item.Ecom import Ecom
from basic.date import get_yesterday



if __name__ == '__main__':
    local_ip = socket.gethostbyname(socket.gethostname())
    if local_ip == '127.0.1.1': # in localhost, UTC+8
        is_UTC0 = False
    else:
        is_UTC0 = True
    ecom = Ecom()
    date_start = get_yesterday(is_UTC0=is_UTC0)
    web_id_all = ecom.fetch_all_web_id()
    # web_id_all = ['cherif']
    for web_id in web_id_all:
        print(f'insert {web_id}')
        ## only update yesterday data (collection is finished)
        df = ecom.insert_daily_report(web_id, mode_insert='yesterday')
        # df = ecom.insert_daily_report(web_id, date_start=date_start, mode_insert='one', is_reformat_product_id=True) ## update everyday
        # df = ecom.insert_daily_report(web_id, date_start=date_start, mode_insert='all', is_reformat_product_id=True) ## collect all to yesterday (use for reset table)



