import DALManager


class OutputDataManager:

    def __init__(self, connection, dbtype="sqlite"):
        """
        JSON data initialization
        :param dbtype:
        """
        # self.dal_manager = None
        # if output_type == "JSON":
        #     self.output_converter = JSONOutputConverter

        if dbtype == "sqlite":
            if "file" not in connection:
                print("SQLite needs file key set")
            else:
                self.dal_manager = DALManager.sqliteDAO(connection["file"])

    def insert_new_file(self, filename, filecontent, userid):
        """
        Inserts new file
        :param filename:
        :param filecontent:
        :param userid:
        :param createdon:
        :return:
        """
        rowid = self.dal_manager.\
            insert_new_file(filename, filecontent, userid)

        output = dict()
        if rowid is not None:
            output["status"] = "success"
            output["result"] = dict()
            output["result"]["id"] = rowid
        else:
            output["status"] = "fail"

        return output

    def insert_new_data_file(self, filename, path, userid):
        """
        Inserts new file
        :param filename:
        :param filecontent:
        :param userid:
        :param createdon:
        :return:
        """
        rowid = self.dal_manager.\
            insert_new_data_file(filename, path, userid)

        output = dict()
        if rowid is not None:
            output["status"] = "success"
            output["result"] = dict()
            output["result"]["id"] = rowid
        else:
            output["status"] = "fail"

        return output

    def update_file(self, fileid, filecontent):
        """
        Update existing file
        :param fileid:
        :param filecontent:
        :return:
        """
        rowcount = self.dal_manager.update_file(fileid, filecontent)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["status"] = "success"
        else:
            output["status"] = "fail"

        return output

    def get_file(self, fileid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        filename, content = self.dal_manager.get_file(fileid)

        output = dict()
        if filename is not None:
            output["status"] = "success"
            output["result"] = dict()
            output["result"]["filename"] = filename
            output["result"]["content"] = content
        else:
            output["status"] = "fail"

        return output

    def get_exec_details(self, execid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        sessionid, fileid, arguments = self.dal_manager.get_exec_details(execid)

        output = dict()
        if fileid is not None:
            output["status"] = "success"
            output["result"] = dict()
            output["result"]["fileid"] = fileid
            output["result"]["sessionid"] = sessionid
            output["result"]["arguments"] = arguments
        else:
            output["status"] = "fail"

        return output

    def get_all_files(self, userid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        all_rows = self.dal_manager.get_all_files(userid)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["status"] = "success"
            output["result"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["id"] = row[0]
                row_dict["filename"] = row[1]
                row_dict["createdon"] = row[2]
                row_dict["modifiedon"] = row[3]

                output["result"].append(row_dict)
        else:
            output["status"] = "fail"

        return output

    def get_all_data_files(self, userid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        all_rows = self.dal_manager.get_all_data_files(userid)
        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["status"] = "success"
            output["result"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["id"] = row[0]
                row_dict["filename"] = row[1]
                row_dict["createdon"] = row[2]
                row_dict["modifiedon"] = row[3]

                output["result"].append(row_dict)
        else:
            output["status"] = "fail"

        return output

    def delete_file(self, fileid):
        """
        Delete file by file id
        :param fileid:
        :return:
        """
        rowcount = self.dal_manager.delete_file(fileid)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["status"] = "success"
        else:
            output["status"] = "fail"

        return output

    def delete_data_file(self, fileid):
        """
        Delete data file by file id
        :param fileid:
        :return:
        """
        rowcount = self.dal_manager.delete_data_file(fileid)

        output = dict()
        if rowcount is not None and rowcount > 0:
            output["status"] = "success"
        else:
            output["status"] = "fail"

        return output

    def insert_executed_file(self, fileid, args, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        rowid = self.dal_manager. \
            insert_executed_file(fileid, args, output_log, output_print, sessionid, userid)

        output = dict()
        if rowid is not None:
            output["status"] = "success"
            output["result"] = dict()
            output["result"]["id"] = rowid
        else:
            output["status"] = "fail"

        return output

    def update_executed_file(self, fileid, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        rowupdated = self.dal_manager. \
            update_executed_file(fileid, output_log, output_print, sessionid, userid)

        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["status"] = "success"
        else:
            output["status"] = "fail"

        return output

    def update_executed_file_wsession(self, execid, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        rowupdated = self.dal_manager. \
            update_executed_file_wsession(execid, output_log, output_print, sessionid, userid)

        output = dict()
        if rowupdated is not None and rowupdated > 0:
            output["status"] = "success"
        else:
            output["status"] = "fail"

        return output

    def get_execution_details(self, fileid):
        """
        Get Execustion history of a file
        :param fileid:
        :return:
        """
        all_rows = self.dal_manager.\
            get_execution_details(fileid)

        output = dict()
        if all_rows is not None and len(all_rows) > 0:
            output["status"] = "success"
            output["result"] = []
            for row in all_rows:
                row_dict = dict()
                row_dict["id"] = row[0]
                row_dict["filecontent"] = row[1]
                row_dict["input_files"] = row[2]
                row_dict["output_files"] = row[3]
                row_dict["output_log"] = row[4]
                row_dict["createdon"] = row[6]
                row_dict["modifiedon"] = row[7]

                output["result"].append(row_dict)
        else:
            output["status"] = "fail"

        return output