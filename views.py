from flask import render_template, send_file
import config
import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
from sparkserve.datamanagement import JSONOutputConverter, DataManager
from sparkserve import livyquery, analyzelog
import time
import json
import shutil
from datetime import datetime



ALLOWED_EXTENSIONS = set(['txt', 'py', 'zip'])

app = Flask(__name__,
            static_folder=config.static_folder,
            template_folder=config.template_folder)

app.config['UPLOAD_FOLDER'] = config.upload_folder

connection = dict()
connection["file"] = config.sqlite_file

livyquery.livy_address = "http://"+config.host+":"+"8998"

userid = 0

def get_last_updated_file(directory):
    max_mtime = 0
    max_file = 'some.txt'
    for dirname, subdirs, files in os.walk(directory):
        for fname in files:
            full_path = os.path.join(dirname, fname)
            mtime = os.stat(full_path).st_mtime
            if mtime > max_mtime:
                max_mtime = mtime
                max_file = fname

    return max_file

@app.route('/')
@app.route('/index')
def index():
    """
    Function to be called for default request
    :return:
    """
    return render_template('index.html')

@app.route('/py/<pagename>')
def get_python_file(pagename):
    """
    Handles any page in the template folder
    :return:
    """
    # show the user profile for that user
    return send_file('/home/cloud/sparkserve/example/%s'% pagename)

@app.route('/<pagename>')
def show_user_profile(pagename):
    """
    Handles any page in the template folder
    :return:
    """
    # show the user profile for that user
    return render_template('%s'% pagename)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/api/uploadcode', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            print('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            #file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            dml = DataManager.OutputDataManager(connection)
            output = dml.insert_new_file(filename, file.stream.getvalue(), userid)
            return JSONOutputConverter.getString(output)

    return ""


@app.route('/api/uploaddata', methods=['POST'])
def upload_data_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'datafile' not in request.files:
            print('No file part')
            return redirect(request.url)
        file = request.files['datafile']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)

            dml = DataManager.OutputDataManager(connection)
            output = dml.insert_new_data_file(filename, config.data_file_path, userid)
            fileid = output["result"]["id"]

            file.save(os.path.join(config.data_file_path, str(fileid)))
            return JSONOutputConverter.getString(output)

    return ""


@app.route('/api/files')
def filelist():
    """
    Handles any page in the template folder
    :return:
    """
    # show the user profile for that user
    dml = DataManager.OutputDataManager(connection)
    output = dml.get_all_files(userid)
    return JSONOutputConverter.getString(output)

@app.route('/api/datafiles')
def data_filelist():
    """
    Handles any page in the template folder
    :return:
    """
    # show the user profile for that user
    dml = DataManager.OutputDataManager(connection)
    output = dml.get_all_data_files(userid)
    return JSONOutputConverter.getString(output)


@app.route('/api/file/<id>')
def get_file(id):
    """
    Handles any page in the template folder
    :return:
    """
    # show the user profile for that user
    dml = DataManager.OutputDataManager(connection)
    output = dml.get_file(id)
    return JSONOutputConverter.getString(output)


@app.route('/api/file/<fileid>/execute', methods=['POST'])
def execute_file(fileid):
    """
    Handles any page in the template folder
    :return:
    """
    dml = DataManager.OutputDataManager(connection)
    arguments = None
    if request.method == 'POST':
        # Insert into db to get unique execution id
        exec_output = dml.insert_executed_file(fileid, "", "", "", -1, userid)
        exec_id = exec_output["result"]["id"]
        rdata = request.data
        args_list = []

        # Parse all the arguments
        if rdata is not None and len(rdata.strip()) > 1:
            data_file_path = config.data_file_path
            data_dict = json.loads(rdata)
            args = data_dict["args"]
            args = sorted(args, key=lambda x: int(x["seq"]))
            for arg in args:
                if arg["type"] == "file":
                    args_list.append(os.path.join(data_file_path,arg["argument"]))
                elif arg["type"] == "output":
                    args_list.append(os.path.join(data_file_path,
                                                  str(exec_id), arg["argument"]))
                else:
                    args_list.append(arg["argument"])

    # get the python file from database and put it into file system
    output = dml.get_file(fileid)
    filecontent = output["result"]["content"]
    filename = output["result"]["filename"]

    file_path = app.config['UPLOAD_FOLDER']+"/"+str(fileid) + ".py"
    text_file = open(file_path, "w")
    text_file.write(filecontent)
    text_file.close()

    # execute python file using livy
    livy_output = livyquery.execute_file(filelocation=file_path,
                                         arguments=args_list)
    livy_output = livy_output.json()

    sessionid = livy_output["id"]

    #time.sleep(1)

    livy_log = livyquery.log_status(sessionid).json()
    livy_log = livy_log["log"]

    job_status = livyquery.job_status(sessionid).json()

    printed_output = analyzelog.get_output(livy_log)

    exec_output = dml.update_executed_file_wsession(exec_id, json.dumps(livy_log),
                                     printed_output,
                                     sessionid, userid)

    # format output
    output = dict()
    output["status"] = "success"
    output["result"] = dict()

    output["result"]["print"] = printed_output
    output["result"]["id"] = exec_id
    output["result"]["state"] = job_status["state"]

    return JSONOutputConverter.getString(output)


@app.route('/api/file/<fileid>/<execid>/status')
def job_status(fileid, execid):
    """
    get the status
    :return:
    """
    output = dict()
    dml = DataManager.OutputDataManager(connection)
    sessionid = None
    # get the session id from execution details
    exec_output = dml.get_exec_details(execid)
    if exec_output["status"] == "success":
        filename = exec_output["result"]["fileid"]
        sessionid = exec_output["result"]["sessionid"]
        arguments = exec_output["result"]["arguments"]
    else:
        output["status"] = "fail"
        output["result"] = dict()
        output["result"]["message"] = "Session id not found"

    status_output = livyquery.job_status(sessionid)
    if status_output.status_code == 404:
        output["status"] = "fail"
        return JSONOutputConverter.getString(output)

    # get log status using livy
    livy_log = livyquery.log_status(sessionid)
    if livy_log.status_code == 404:
        output["status"] = "fail"
        output["result"] = dict()
        output["result"]["message"] = "Session not found"
        return JSONOutputConverter.getString(output)

    livy_log = livy_log.json()["log"]

    printed_output = analyzelog.get_output(livy_log)

    dml.update_executed_file_wsession(execid, json.dumps(livy_log),
                             printed_output,
                             sessionid, userid)

    output["status"] = "success"
    output["result"] = dict()

    output["result"]["print"] = printed_output
    output["result"]["id"] = execid
    output["result"]["state"] = status_output.json()["state"]
    output["result"]["log"] = livy_log
    output["result"]["logfile"] = get_last_updated_file(config.spark_events_path)

    return JSONOutputConverter.getString(output)


@app.route('/api/file/<execid>/output')
def get_output(execid):
    """
    get the status
    :return:
    """
    output = dict()
    dml = DataManager.OutputDataManager(connection)

    sessionid = None

    # get the session id from execution details
    exec_output = dml.get_exec_details(execid)
    if exec_output["status"] == "success":
        output["status"] = "success"
        output["result"] = dict()
    else:
        output["status"] = "fail"
        output["result"] = dict()
        output["result"]["message"] = "Session id not found"

    return JSONOutputConverter.getString(output)

@app.route('/api/file/<execid>/output/download')
def download_output(execid):
    """
    get the status
    :return:
    """
    output = dict()
    dml = DataManager.OutputDataManager(connection)

    sessionid = None

    # get the session id from execution details
    exec_output = dml.get_exec_details(execid)
    if exec_output["status"] == "success":
        filename = exec_output["result"]["fileid"]
        sessionid = exec_output["result"]["sessionid"]
        arguments = exec_output["result"]["arguments"]
        output_folder = os.path.join(config.data_file_path,str(execid))
        if os.path.exists(output_folder):
            output_file_path = os.path.join(config.data_file_path,"output"+str(datetime.now()))
            shutil.make_archive(output_file_path, 'zip', output_folder)
            return send_file(output_file_path+".zip")