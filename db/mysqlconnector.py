import os,json
import pymysql
from sqlalchemy import create_engine, Table, MetaData, insert
import socket
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.mysql import insert
from sshtunnel import SSHTunnelForwarder

class MysqlConnector:
    engine = {}
    Session = {}
    def __init__(self, service, is_ssh=False):
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.service = service
        self.local_ip = socket.gethostbyname(socket.gethostname())
        if service not in MysqlConnector.engine:
            # BASE_DIR = self.BASE_DIR
            with open(os.path.join(self.BASE_DIR, "db", "settings.json")) as default_config_file:
                config = json.load(default_config_file)
                self.config = config
                print(config)
            if ((("SSH" in config["mysql"][service]) & (self.local_ip == '127.0.1.1')) | is_ssh) : ## in local pc
                mysqlsql_uri = self.__ssh_forwarder(config["mysql"][service])
                # print(mysqlsql_uri)
            else: ## in server, no need to SSH
                mysqlsql_uri = self.__compose_uri(config["mysql"][service])
            engine = create_engine(mysqlsql_uri, echo=False, pool_pre_ping=True, pool_recycle=1800, poolclass=NullPool)
            MysqlConnector.engine[service] = engine
            MysqlConnector.Session[service] = sessionmaker(bind=engine)

        connection = MysqlConnector.engine[service].connect()
        self.session = MysqlConnector.Session[service](bind=connection)


    def get_session(self):
        return self.session

    def session_close(self):
        self.session.get_bind().close()
        self.session.close()   
        if 'server' in dir(self):
            self.server.stop()

    def query(self, *entities, **kwargs):
        return self.session.query(*entities, **kwargs)

    def insert(self, *entities, **kwargs):
        return self.session.insert(*entities, **kwargs)

    def execute_raw_sql(self, *entities, **kwargs):
        return self.session.execute(*entities, **kwargs)

    def insert_table(self, table_name, list_dict):
        metadata = MetaData(self.engine)
        table = Table(table_name, metadata, autoload=True)
        # table = Table(table_name, metadata)
        print(table.insert())
        with self.engine.connect() as conn:
            conn.execute(table.insert(), list_dict)
            conn.commit()

    def __compose_uri(self, config):
        host = config["MYSQL_HOST"]
        port = config["MYSQL_PORT"]
        if port:
            host = "{}:{}".format(host, port)
        
        user = config["MYSQL_USER"]
        password = config["MYSQL_PASSWORD"]
        if password:
            user = "{}:{}".format(user, password)
        # return "mysql+pymysql://{}@{}/{}??charset=utf8mb4".format(user, host, config["MYSQL_DB"])
        return "mysql+pymysql://{}@{}/{}?".format(user, host, config["MYSQL_DB"])

    def __ssh_forwarder(self, config):
        self.server = SSHTunnelForwarder(
             (config["SSH"]["HOST"], config["SSH"]["PORT"]),
             ssh_password = config["SSH"].get("PASSWORD", None),
             ssh_username = config["SSH"]["USER"],
             # ssh_pkey = config["SSH"].get("PRIVATEKEY", None),
             # ssh_pkey = './db/likr.pem',
             ssh_pkey = os.path.join(self.BASE_DIR, "db", "likr.pem"),
             remote_bind_address=(config["MYSQL_HOST"], config["MYSQL_PORT"])
        )
        #self.TUNNEL_TIMEOUT = 20.0
        self.server.start()

        host = '127.0.0.1'
        port = self.server.local_bind_port
        if port:
            host = "{}:{}".format(host, port)
        user = config["MYSQL_USER"]
        password = config["MYSQL_PASSWORD"]
        if password:
            user = "{}:{}".format(user, password)
        # return "mysql+pymysql://{}@{}/{}??charset=utf8mb4".format(user, host, config["MYSQL_DB"])
        return "mysql+pymysql://{}@{}/{}?".format(user, host, config["MYSQL_DB"])
