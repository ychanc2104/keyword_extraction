import socket
from predict_hot_item.Ecom import Ecom
from basic.date import get_yesterday, check_is_UTC0
from update_cdp_predict_revenue import save_predict_revenue


if __name__ == '__main__':

    is_UTC0 = check_is_UTC0()
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

    ## update predict revenue
    save_predict_revenue()