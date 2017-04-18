import json, requests

livy_address = None # 'http://localhost:8998'


def execute_file(filelocation, arguments):
    """
    Execute file in livy server
    :param filelocation:
    :return:
    """
    host = livy_address
    data = {'file': filelocation, "args":arguments}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    return r


def job_status(sessionid):
    """
    Execute file in livy server
    :param filelocation:
    :return:
    """
    host = livy_address
    headers = {'Content-Type': 'application/json'}
    r = requests.get(host + '/batches/'+str(sessionid), headers=headers)
    return r

def log_status(sessionid):
    """
    Execute file in livy server
    :param filelocation:
    :return:
    """
    host = livy_address
    headers = {'Content-Type': 'application/json'}
    r = requests.get(host + '/batches/'+str(sessionid)+"/log", headers=headers)
    return r