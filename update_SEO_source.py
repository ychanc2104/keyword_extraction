from gAPI.gconsole import GoogleSearchConsole
from basic.decorator import timing
from db.mysqlhelper import MySqlHelper


@timing
def fetch_SEO_siteUrl_web_id():
    query = "SELECT web_id, siteUrl FROM seo_web_id_table where gconsole_enable=1"
    print(query)
    data = MySqlHelper('roas_report').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    sitUrl_all = [d[1] for d in data]
    return web_id_all, sitUrl_all

if __name__ == '__main__':
    ## setup parameters
    # siteUrl = 'https://www.nanooneshop.com/' ## web_id = 'nanooneshop'
    # web_id = 'nanooneshop'

    web_id_all, sitUrl_all = fetch_SEO_siteUrl_web_id()
    g_search = GoogleSearchConsole()
    for web_id, siteUrl in zip(web_id_all, sitUrl_all):
        g_search.update_4db(web_id, siteUrl, path_ads_config='./gAPI/google-ads.yaml')

