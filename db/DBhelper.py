import os, json, socket, math, traceback
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder
from definitions import ROOT_DIR
from basic import logging_local
from log_utils import error_log
import pandas as pd

log_foler = os.path.dirname(os.path.abspath(__file__))

class DBhelper:
    def __init__(self, service, is_ssh=False):
        # self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.BASE_DIR = ROOT_DIR
        self.service = service
        self.is_ssh = is_ssh
        self.local_ip = socket.gethostbyname(socket.gethostname())
        self.config = self._read_config()
        if self._check_ssh():
            self.mysqlsql_uri = self._ssh_forwarder(self.config["mysql"][service])
        else:
            self.mysqlsql_uri = self._compose_uri(self.config["mysql"][service])
        self.engine = create_engine(self.mysqlsql_uri, echo=False,
                                    pool_pre_ping=True, pool_recycle=1800,
                                    poolclass=NullPool)
        self.connection = self.engine.connect()
        self.session = sessionmaker(bind=self.engine)(bind=self.connection)
    @logging_local(log_foler, log_traceback=False)
    def ExecuteSelect(self, query, disconnect=True):
        '''
            输入查詢SQL語句
            输出：查詢的結果
        '''
        result = self.session.execute(query)
        data = result.fetchall()
        if disconnect:
            self.session_close()
        return data

    def ExecuteUpdate(self, query, data, disconnect=True):
        '''
            输入非查詢SQL語句
            输出：受影響的行數
        '''
        count = 0
        try:
            result = self.execute_raw_sql(query, data)
            count = result.rowcount
            self.session.commit()
        except Exception as e:
            error_log(e, ROOT_DIR=log_foler)
            print(e)
            self.session.rollback()
        if disconnect:
            self.session_close()
        print(f"affected rows: {count}")
        return count

    def ExecuteDelete(self, query, disconnect=True):
        '''
            输入非查詢SQL語句
            输出：受影響的行數
        '''
        count = 0
        try:
            result = self.execute_raw_sql(query)
            count = result.rowcount
            self.session.commit()
        except Exception as e:
            error_log(e, ROOT_DIR=log_foler)
            print(e)
            self.session.rollback()
        if disconnect:
            self.session_close()
        print(f"affected rows: {count}")
        return count

    ## will lock table while optimizing
    def ExecuteOptimize(self, table):
        try:
            self.execute_raw_sql(f"optimize table {table}")
            self.session.commit()
        except Exception as e:
            error_log(e, ROOT_DIR=log_foler)
            print(e)
            self.session.rollback()
        self.session_close()
        print("finish optimizing table")

    @staticmethod
    def ExecuteUpdatebyChunk(df, db, table='', query=None, SQL_action=1,
                             update_col_list=[], chunk_size=100000, is_ssh=False):
        """
        iteratively update sql by chunk_size

        Parameters
        ----------
        df: DataFrame
        db: str: schema name
        table: str: table name
        query: str: operation of SQL, default using REPLACE INTO
        SQL_action: int: not used if query!=None, other:insert on duplicate key, 0:replace into
        update_col_list: list: not used if query!=None, for insert on duplicate key
        Returns
        -------

        """
        dbhelper = DBhelper(db, is_ssh=is_ssh)
        if df.shape[0]==0:
            print("no available dat to import")
        else:
            if query==None:
                if SQL_action==0:
                    query = dbhelper.generate_update_SQLquery(df, table)
                else:
                    update_col_list = df.columns if update_col_list==[] else update_col_list
                    query = dbhelper.generate_insertDup_SQLquery(df, table, update_col_list)
            # query = dbhelper.generate_update_SQLquery(df, table)
            dict_list = df.to_dict('records')
            n = int(math.ceil(len(dict_list)/chunk_size))
            if n<=1: ## directly import all
                print(f"size {len(dict_list)}, directly import all data to sql table")
                dbhelper.ExecuteUpdate(query, dict_list, disconnect=True)
            else:
                # print(f"size {len(dict_list)}, import {n} times")
                for i in range(n):
                    print(f"size {len(dict_list)}, import {i+1}/{n} times")
                    if i==n-1: ## last round
                        data = dict_list[i*chunk_size:]
                        # print(data)
                        dbhelper.ExecuteUpdate(query, data, disconnect=True)
                    else:
                        data = dict_list[i*chunk_size:(i+1)*chunk_size]
                        # print(data)
                        dbhelper.ExecuteUpdate(query, data, disconnect=False)

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
    def generate_insertDup_SQLquery(df, table_name:str, update_col_list:list):
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
            if i == len(update_col_list) - 1:
                query += f"{col} = VALUES({col})"
            else:
                query += f"{col} = VALUES({col}),"

        print(f"auto-generating SQL script, \n{query}")
        return query

    #     query = f"UPDATE cdp_tracking_settings SET web_id=:web_id,avg_shipping_price=:avg_shipping_price,avg_total_price=:avg_total_price WHERE web_id=:web_id"
    @staticmethod
    def generate_updateTable_SQLquery(table_name, update_col_list, where_col_list):
        query = f"UPDATE {table_name} SET "
        # query = "REPLACE INTO google_search_console_device (web_id, clicks, impressions, position, device, date) VALUES (:web_id, :clicks, :impressions, :position, :device, :date)"
        for i,col in enumerate(update_col_list):
            if i == len(update_col_list) - 1:
                query += f"{col}=:{col}"
            else:
                query += f"{col}=:{col},"
        query += " WHERE "
        for i,col in enumerate(where_col_list):
            if i == len(where_col_list) - 1:
                query += f"{col}=:{col}"
            else:
                query += f"{col}=:{col} AND "
        print(f"auto-generating SQL script, \n{query}")
        return query

    def _check_ssh(self):
        c0 = ("SSH" in self.config["mysql"][self.service]) & (self.local_ip == '127.0.1.1')
        return c0 | self.is_ssh

    @staticmethod
    def _read_config(path=None):
        if path==None:
            path = os.path.join(ROOT_DIR, "db", "settings.json")
        with open(path) as config_file:
            config = json.load(config_file)
            return config

    def get_session(self):
        return self.session

    def session_close(self):
        self.session.get_bind().close()
        if 'server' in dir(self):
            self.server.stop()

    def execute_raw_sql(self, *entities, **kwargs):
        return self.session.execute(*entities, **kwargs)

    def _compose_uri(self, config):
        host = config["MYSQL_HOST"]
        port = config["MYSQL_PORT"]
        if port:
            host = f"{host}:{port}"
        user = config["MYSQL_USER"]
        password = config["MYSQL_PASSWORD"]
        schema = config['MYSQL_DB']
        if password:
            user = f"{user}:{password}"
        return f"mysql+pymysql://{user}@{host}/{schema}"

    def _ssh_forwarder(self, config):
        self.server = SSHTunnelForwarder(
            (config["SSH"]["HOST"], config["SSH"]["PORT"]),
            ssh_password=config["SSH"].get("PASSWORD", None),
            ssh_username=config["SSH"]["USER"],
            ssh_pkey=os.path.join(self.BASE_DIR, "db", "likr.pem"),
            remote_bind_address=(config["MYSQL_HOST"], config["MYSQL_PORT"])
        )
        # self.TUNNEL_TIMEOUT = 20.0
        self.server.start()
        host = '127.0.0.1'
        port = self.server.local_bind_port
        if port:
            host = f"{host}:{port}"
        user = config["MYSQL_USER"]
        password = config["MYSQL_PASSWORD"]
        schema = config['MYSQL_DB']
        if password:
            user = f"{user}:{password}"
        return f"mysql+pymysql://{user}@{host}/{schema}"


if __name__ == '__main__':

    x = DBhelper('tracker')
    # query = "SELECT *x FROM tracker.clean_event_purchase order by id desc limit 100"
    df = pd.DataFrame([{'web_idx': 'xxx'}]*3)
    query = x.generate_update_SQLquery(df, 'raw_event')
    x.ExecuteUpdate(query, df.to_dict('records'))
    # x.session_close()
    # data = x.ExecuteSelect(query)


