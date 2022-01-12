import numpy as np

##  moving average filter
def MA(data, window, mode='sliding'):
    data = np.array(data)
    n = len(data)
    data_filter = []
    m = int(window)

    if mode == 'sliding':
        for i in range(n):
            if i < m:
                data_filter += [np.mean(data[:(i+1)])]
            else:
                data_filter += [np.mean(data[i-window+1:i+1])]
    elif mode == 'fixing':
        iteration = n//window
        for i in range(iteration):
            data_filter += [np.mean(data[i*window:(i+1)*window])]

    return np.array(data_filter)

def filterListofDictByList(dict_list, key_list, value_list):
    """

    :param dict_list:
    :param key_list:
    :param value_list:
    :return:
    """
    for key,value in zip(key_list, value_list):
        dict_list = filterListofDict(dict_list, key, value)
    return dict_list

def filterListofDictByDict(dict_list, dict_criteria):
    """

    :param dict_list:
    :param key_list:
    :param value_list:
    :return:
    """
    for key,value in dict_criteria.items():
        dict_list = filterListofDict(dict_list, key, value)
    return dict_list


def filterListofDict(dict_list, key, value=None):
    """

    :param dict_list: data set
    :param key: key of dict which you want to filter
    :param value: if value=None, do not filter value
    :return:
    """
    if value==None:
        return list(filter(lambda x: key in x.keys(), dict_list))
    else:
        return list(filter(lambda x: value in x[key], filter(lambda x: key in x.keys(), dict_list)))


