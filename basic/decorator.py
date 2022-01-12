import time
from logger import slackBot
from functools import wraps

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
def logging_channels(channel_name_list):
    def logging(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                message = f"Error Message: ```{e}```\nTrigger By Function: ```{func.__name__}{args}``` Raise Error in Path: ```{func.__code__.co_filename}```\nPlase Check"
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
        except Exception as e:
            message = f"error message: ```{e}```\ntrigger by function: ```{func.__name__}{args}``` raise error in path: ```{func.__code__.co_filename}```\nPlase check"
            slackBot("clare_test").send_message(message)
            print(message)
    return wrapper


@logging_channels(["clare_test"])
def divide(x,y):
    return x/y

if __name__ == "__main__":

    a = divide(10,1)