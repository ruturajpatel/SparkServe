from sparkserve.datamanagement import DALManager
import datetime
import config

dm = DALManager.sqliteDAO(config.sqlite_file)


def test_tablecreation():
    """
    Test all necessary table creation
    :return:
    """
    dm.configure_tables()


def test_insert_new_file():
    """
    Test all necessary table creation
    :return:
    """
    filename="testsqlite.py"
    filecontent = "import test"
    userid = 0
    fileid = dm.insert_new_file(filename, filecontent, userid)
    print("New File id:"+ str(fileid))

def test_update_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=4
    filecontent = "import testupdated"

    dm.update_file(fileid, filecontent)
    print("File updated")


def test_get_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=1
    filename, filecontent = dm.get_file(fileid)
    print("File Name:{name} content:{content}".
          format(name=filename, content=filecontent))


def test_get_all_files():
    """
    Test all necessary table creation
    :return:
    """
    userid=0
    all_files = dm.get_all_files(userid)
    print("File List")
    print(all_files)


def test_delete_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid = 1
    dm.delete_file(fileid)
    print("File deleted")


def test_save_executed_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=1
    output_log = "Test output log"
    output_print = "Test output print"
    userid = 0
    dm.save_executed_file(fileid, output_log, output_print, userid)
    print("File execution details saved")

def test_get_execution_details():
    """
    Test all necessary table creation
    :return:
    """
    fileid=1
    all_rows = dm.get_execution_details(fileid)
    print("Execustion details for:" + str(fileid))
    print(all_rows)


print("Test: Table creation")
test_tablecreation()

# print("Test: Insert new data1")
# test_insert_new_file()
#
# print("Test: Insert new data2")
# test_insert_new_file()

# print("Test: Get data")
# test_get_file()
#
# print("Test: Update file data")
# test_update_file()
#
# print("Test: Delete file")
# test_delete_file()

# print("Test: Get all file")
# test_get_all_files()

# print("Test: Save executed file")
# test_save_executed_file()

# print("Test: Get executed file")
# test_get_execution_details()

