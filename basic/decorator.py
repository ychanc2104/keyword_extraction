import time
from log_utils import slackBot, error_log
from functools import wraps
import traceback

def timing(func):
    @wraps(func)
    def time_count(*args, **kwargs):
        t_start = time.time()
        values = func(*args, **kwargs)
        t_end = time.time()
        print (f"{func.__name__} time consuming:  {(t_end - t_start):.3f} seconds")
        return values
    return time_count

## log decorator with assign channel
def logging_channels(channel_name_list=[], report_args=True, n_args=2, save_slack=True, save_local=False,
                     log_traceback=True, ROOT_DIR=None):
    def logging(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    error = traceback.format_exc()
                else:
                    error = e
                # error = traceback.format_exc()
                if report_args:
                    message = f"Error Message: ```{error}```\nTrigger By Function: ```{func.__name__}{args[:n_args]}...``` Raise Error in Path: ```{func.__code__.co_filename}```\nPlase Check"
                else:
                    message = f"Error Message: ```{error}```\nTrigger By Function: ```{func.__name__}``` Raise Error in Path: ```{func.__code__.co_filename}```\nPlase Check"
                if save_local:
                    error_log(error, ROOT_DIR=ROOT_DIR)
                if save_slack:
                    slackBot(channel_name_list).send_message(message)
                print(message)
        return wrapper
    return logging


## log decorator
def logging(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            error = traceback.format_exc()
            message = f"error message: ```{error}```\ntrigger by function: ```{func.__name__}{args[:2]}...``` raise error in path: ```{func.__code__.co_filename}```\nPlase check"
            slackBot("clare_test").send_message(message)
            print(message)
    return wrapper


@logging_channels(["clare_test"], save_local=True)
def divide(x,y):
    return x/y


def logging_local(ROOT_DIR=None, filefolder='log', filename='error.log', log_traceback=True):
    def logging_local_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    error = traceback.format_exc()
                else:
                    error = e
                error_log(error, ROOT_DIR=ROOT_DIR, filefolder=filefolder, filename=filename)
                print(error)
        return wrapper
    return logging_local_wrapper


if __name__ == "__main__":

    a = divide(10,0)