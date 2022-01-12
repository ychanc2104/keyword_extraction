import os,json,datetime,logging
from logging.handlers import RotatingFileHandler

class Logger:
    def __init__(self, suffix=''):
        project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(project_path, "db", "settings.json")) as config_file:
            config = json.load(config_file)
            project_name = config["project_name"]
            env = config["env"]
            log_basename_prefix = "{}_{}".format(project_name, env)
            log_dir = os.path.join(project_path, "log")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_path = os.path.join(log_dir, log_basename_prefix)

            handler = RotatingFileWithDateHandler(filename_prefix=log_path, maxBytes=config["log_max_bytes"], backupCount=config["log_backup_count"])
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
            handler.setFormatter(formatter)
            self.logger = logging.getLogger(suffix)
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(handler)


class RotatingFileWithDateHandler(RotatingFileHandler):
    def __init__(self, filename_prefix, mode='a+', maxBytes=0, backupCount=0, encoding=None, delay=False):
        self.today = str(datetime.date.today())
        self.filename_prefix = filename_prefix
        #filename = "{}{}.log".format(self.filename_prefix, datetime.datetime.now().strftime('%Y-%m-%d'))
        filename = "{}.log".format(self.filename_prefix)
        RotatingFileHandler.__init__(self, filename, mode=mode, maxBytes=maxBytes, backupCount=backupCount, encoding=encoding, delay=delay)
        
    def shouldRollover(self, record):
        current_date = str(datetime.date.today())
        if current_date != self.today:
            self.today = current_date
            #self.baseFilename = "{}{}.log".format(self.filename_prefix, datetime.datetime.now().strftime('%Y-%m-%d'))
            self.baseFilename = "{}.log".format(self.filename_prefix)
            return 1
        return RotatingFileHandler.shouldRollover(self, record)

if __name__ == '__main__':
    x=1