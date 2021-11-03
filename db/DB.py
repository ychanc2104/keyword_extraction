import pymysql
import configparser
import socket
from basic.decorator import timing

class DB:
    def __init__(self, config_path='./db/sysconfig_local.ini'): ## in_local=True, run in localhost
        self.config = self.__read_config(config_path)

    def connect_mysql(self, server_name, db_name): #network_mode: 0 for running in gcp, others for running in local
        config = self.config
        local_ip = socket.gethostbyname(socket.gethostname())
        if local_ip == '127.0.1.1': ## not in rhea-vm0
            host = config.get(server_name, 'host')
        else:
            host = config.get(server_name, 'localhost')
        account = config.get(server_name, 'user')
        password = config.get(server_name, 'passwd')
        db = pymysql.connect(host=host, user=account, password=password, database=db_name, charset='utf8')
        return db

    @timing
    def __read_config(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        return config
