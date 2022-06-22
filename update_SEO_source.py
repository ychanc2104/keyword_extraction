from gAPI import GoogleSearchConsole
from basic import timing
from db import DBhelper

@timing
def fetch_SEO_siteUrl_web_id():
    query = "SELECT web_id, siteUrl FROM seo_web_id_table where gconsole_enable=1"
    print(query)
    data = DBhelper('roas_report').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    sitUrl_all = [d[1] for d in data]
    return web_id_all, sitUrl_all

## update google_search_console_query, google_search_console_device, google_search_console_page, google_search_console_page_query
if __name__ == '__main__':
    ## setup parameters
    web_id_all, sitUrl_all = fetch_SEO_siteUrl_web_id()
    g_search = GoogleSearchConsole()
    for web_id, siteUrl in zip(web_id_all, sitUrl_all):
        ############### update today(T), T-1, T-2, T-3 four days data ###############
        g_search.update_4db(web_id, siteUrl)
        ############### update today(T), T-1, T-2, T-3 four days data ###############

        ############### init db ###############
        # web_id = 'i3fresh'
        # siteUrl = 'https://i3fresh.tw/'
        # date_start = '2021-11-07'
        # date_end = '2021-11-15' ## To today-3
        # g_search.save_4db_by_date(web_id, siteUrl, date_start, date_end)
        ############### init db ###############

