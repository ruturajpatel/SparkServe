from sparkserve.datamanagement import DataManager
import config

connection = dict()
connection["file"] = config.sqlite_file

dm = DataManager.OutputDataManager(connection)

def test_insert_new_file():
    """
    Test all necessary table creation
    :return:
    """
    filename="testsqlite.py"
    filecontent = "import test"
    userid = 0
    print(dm.insert_new_file(filename, filecontent, userid))

def test_update_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=4
    filecontent = "import testupdated"

    print(dm.update_file(fileid, filecontent))


def test_get_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=2
    print(dm.get_file(fileid))


def test_get_all_files():
    """
    Test all necessary table creation
    :return:
    """
    userid=0
    print(dm.get_all_files(userid))



def test_delete_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid = 5
    print(dm.delete_file(fileid))


def test_save_executed_file():
    """
    Test all necessary table creation
    :return:
    """
    fileid=2
    output_log = "Test output log"
    output_print = "Test output print"
    userid = 0
    print(dm.save_executed_file(fileid, output_log, output_print, userid))
    #print("File execution details saved")

def test_get_execution_details():
    """
    Test all necessary table creation
    :return:
    """
    fileid=2
    print(dm.get_execution_details(fileid))


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

print("Test: Get executed file")
test_get_execution_details()

