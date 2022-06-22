from gAPI import GoogleSearchConsole
from basic import timing, curdate, datetime_to_str
from db import DBhelper
import argparse

def fetch_siteUrl_by_web_id(web_id):
    query = f"SELECT siteUrl FROM seo_web_id_table where web_id='{web_id}'"
    print(query)
    data = DBhelper('roas_report').ExecuteSelect(query)
    return data[0][0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update historical ROAS')
    parser.add_argument("-w", "--web_id",
                        help="web_id wanted to be update, if None, fetch all")
    parser.add_argument("-d1", "--date_start",
                        help="import SEO sources from date_start to date_end, format: '2022-01-01', default using 3 days before")
    parser.add_argument("-d2", "--date_end",
                        help="import SEO sources from date_start to date_end, format: '2022-01-01', default using 3 days before")
    args = parser.parse_args()
    web_id = args.web_id

    date_start = args.date_start
    date_end = args.date_end
    if not date_start or not date_end:
        # both None case
        date_start = datetime_to_str(curdate(utc=8-24*3)) if not date_start else date_start
        date_end = datetime_to_str(curdate(utc=8-24*3)) if not date_end else date_end

    web_id = 'draimior' if not web_id else web_id
    siteUrl = fetch_siteUrl_by_web_id(web_id)
    df_all = GoogleSearchConsole().save_4db_by_date(web_id, siteUrl, date_start, date_end)

