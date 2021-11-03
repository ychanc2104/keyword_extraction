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