import pandas as pd

from db.mysqlconnector import MysqlConnector
from db.logger import Logger
from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy import Column, Integer, String, DATETIME
class MySqlHelper:

    def __init__(self, CONN_INFO, is_ssh=False):
        self.logger = Logger("mysql").logger
        self.sql_connector = MysqlConnector(CONN_INFO, is_ssh=is_ssh)
        self.CONN_INFO = CONN_INFO
        self.is_ssh = is_ssh

    def ExecuteDelete(self, query, disconnect=False):
        '''
            输入非查詢SQL語句
            输出：受影響的行數
        '''
        count = 0
        try:
            result = self.sql_connector.execute_raw_sql(query)
            count = result.rowcount
        except Exception as e:
            self.logger.info(e)
            self.sql_connector.get_session().rollback()
        else:
            self.sql_connector.get_session().commit()
        if disconnect:
            self.sql_connector.get_session().get_bind().close()
            self.sql_connector.get_session().close()
        return count

    def ExecuteUpdate(self, *entities, **kwargs):
        '''
            输入非查詢SQL語句
            输出：受影響的行數
        '''
        count = 0
        try:   
            result = self.sql_connector.execute_raw_sql(*entities, **kwargs)
            count = result.rowcount
        except Exception as e:
            self.logger.info(e)
            self.sql_connector.get_session().rollback()
        else:    
            self.sql_connector.get_session().commit()                     
        self.sql_connector.get_session().get_bind().close()
        self.sql_connector.get_session().close()
        return count

    def ExecuteInsert(self, table_name, list_dict):
        '''
            输入非查詢SQL語句
            输出：受影響的行數
        '''
        engine = self.sql_connector.engine[self.CONN_INFO]
        metadata = MetaData(engine)
        table = Table(table_name, metadata, autoload=True)
        self.table = table
        print(insert(table))
        # self.sql_connector.Session[self.CONN_INFO].execute(insert(table), list_dict)
        try:
            with engine.connect() as conn:
                conn.execute(insert(table), list_dict)
                conn.commit()
        except Exception as e:
            self.logger.info(e)
        self.sql_connector.get_session().close()
        # count = 0
        # try:
        #     self.sql_connector.insert_table(table_name, list_dict)
        #     # count = result.rowcount
        # except Exception as e:
        #     self.logger.info(e)
        #     self.sql_connector.get_session().rollback()
        # return count
        

    def ExecuteSelect(self, *entities, **kwargs):
        '''
            输入查詢SQL語句
            输出：查詢的結果
        '''
        result = self.sql_connector.execute_raw_sql(*entities, **kwargs)
        data = result.fetchall()       
        self.sql_connector.get_session().get_bind().close()
        self.sql_connector.get_session().close()
        return data

    def close_sql_session(self):
        self.sql_connector.get_session().get_bind().close()
        self.sql_connector.get_session().close()

    ## support INSERT and REPLACE INTO
    @staticmethod
    def generate_update_SQLquery(df, table_name, SQL_ACTION="REPLACE INTO"):
        columns = df.columns.values
        n_col = len(columns)
        query = f"{SQL_ACTION} {table_name}"
        # query = "REPLACE INTO google_search_console_device (web_id, clicks, impressions, position, device, date) VALUES (:web_id, :clicks, :impressions, :position, :device, :date)"
        params, bind_params = "(", "("
        for i, col in enumerate(columns):
            if i == (n_col - 1):  ## reach end
                params += f"{col})"
                bind_params += f":{col})"
            else:
                params += f"{col},"
                bind_params += f":{col},"
        query = f"{query} {params} VALUES {bind_params}"
        print(f"auto-generating SQL script, \n{query}")
        return query

    ## support INSERT and REPLACE INTO
    # """
    # query = ''' INSERT INTO web_push.usertag_uuid_sorted (web_id, uuid, keywordList, keywordFreq, viewArticles) VALUES (:web_id, :uuid, :keywordList, :keywordFreq, :viewArticles)
    #         ON DUPLICATE KEY UPDATE keywordList = VALUES(keywordList),
    #                                 keywordFreq = VALUES(keywordFreq),
    #                                 viewArticles = VALUES(viewArticles)
    #     '''
    # """
    @staticmethod
    def generate_insertDup_SQLquery(df, table_name, update_col_list):
        columns = df.columns.values
        n_col = len(columns)
        query = f"INSERT INTO {table_name}"
        # query = "REPLACE INTO google_search_console_device (web_id, clicks, impressions, position, device, date) VALUES (:web_id, :clicks, :impressions, :position, :device, :date)"
        params, bind_params = "(", "("
        for i, col in enumerate(columns):
            if i == (n_col - 1):  ## reach end
                params += f"{col})"
                bind_params += f":{col})"
            else:
                params += f"{col},"
                bind_params += f":{col},"

        query = f"{query} {params} VALUES {bind_params} ON DUPLICATE KEY UPDATE "
        for i, col in enumerate(update_col_list):
            if i==len(update_col_list)-1:
                query += f"{col} = VALUES({col})"
            else:
                query += f"{col} = VALUES({col}),"

        print(f"auto-generating SQL script, \n{query}")
        return query

## unit test
if __name__ == '__main__':

    query = f"""
            SELECT web_id,keyword_gtrend,keyword_ecom,product_id,title,score,score_damerau,weight_cosine FROM roas_report.seo_sim_products where web_id='i3fresh' and keyword_gtrend='龍涎香'
            """
    print(query)
    data = MySqlHelper("roas_report").ExecuteSelect(query)

    df = pd.DataFrame(data, columns=['web_id','keyword_gtrend','keyword_ecom','product_id','title','score','score_damerau','weight_cosine'])
    query = MySqlHelper.generate_insertDup_SQLquery(df, 'seo_sim_products', update_col_list=['score_damerau', 'weight_cosine'])
    df['score_damerau'] = 1 #1
    df['weight_cosine'] = 1 #1

    MySqlHelper("roas_report").ExecuteUpdate(query, df.to_dict('records'))



