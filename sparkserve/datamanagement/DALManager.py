import sqlite3
import datetime


class sqliteDAO:

    def __init__(self, sqlite_file):
        """
        Initialization of sqlite connection
        :param sqlite_file:
        """
        self.conn = sqlite3.connect(sqlite_file)
        self.cursor = self.conn.cursor()

    def configure_tables(self):
        """
        Configure necessary tables
        :return:
        """
        # File table creation
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS {tn} (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              filename TEXT,
                              content TEXT,
                              userid INTEGER,
                              createdon DATETIME,
                              modifiedon DATETIME,
                              isdeleted BOOLEAN DEFAULT 0,
                              deletedon DATETIME
                            )"""
                            .format(tn="files"))
        
        # execution table creation
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS {tn} (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              fileid INTEGER,
                              filecontent TEXT,
                              sessionid INTEGER,
                              arguments TEXT,
                              outputlog TEXT,
                              print_output TEXT,
                              userid INTEGER,
                              createdon DATETIME,
                              modifiedon DATETIME
                            )"""
                            .format(tn="execution"))
        # execution table creation
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS {tn} (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              filename TEXT,
                              path TEXT,
                              userid INTEGER,
                              createdon DATETIME,
                              modifiedon DATETIME,
                              isdeleted BOOLEAN DEFAULT 0,
                              deletedon DATETIME
                            )"""
                            .format(tn="datafile"))

    def insert_new_file(self, filename, filecontent, userid):
        """
        Inserts new file
        :param filename:
        :param filecontent:
        :param userid:
        :param createdon:
        :return:
        """
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO files(filename, content, userid, createdon)
                            VALUES(?,?,?,?)
                            """, (filename, filecontent, userid, createdon))
        rowid = self.cursor.lastrowid
        self.conn.commit()

        return rowid

    def insert_new_data_file(self, filename, path, userid):
        """
        Inserts new data file
        :param filename: name of the file
        :param path: existing path of the file
        :param userid:
        :return:
        """
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO datafile(filename, path, userid, createdon)
                            VALUES(?,?,?,?)
                            """, (filename, path, userid, timenow))
        rowid = self.cursor.lastrowid
        self.conn.commit()

        return rowid

    def update_file(self, fileid, filecontent):
        """
        Update existing file
        :param fileid:
        :param filecontent:
        :return:
        """
        updatedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE files SET content=? , modifiedon = ?
                            WHERE id =? AND isdeleted = 0
                            """, (filecontent, updatedon, fileid))
        rowcount = self.cursor.rowcount
        self.conn.commit()

        return rowcount

    def get_file(self, fileid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        self.cursor.execute("""
                            SELECT filename, content FROM files
                            WHERE id=? AND isdeleted = 0
                            """, (fileid,))
        all_rows = self.cursor.fetchall()

        # Return filename and content
        if len(all_rows) > 0:
            row = all_rows[0]
            return row[0], row[1]

        return None, None

    def get_exec_details(self, execid):
        """
        Get file name and content
        :param exec: id of the file to fetch
        :return: name, content
        """
        self.cursor.execute("""
                            SELECT sessionid, fileid, arguments FROM execution
                            WHERE id=?
                            """, (execid,))
        all_rows = self.cursor.fetchall()

        # Return filename and content
        if len(all_rows) > 0:
            row = all_rows[0]
            return row[0], row[1], row[2]

        return None, None, None

    def get_all_files(self, userid):
        """
        Get file name and content
        :param fileid: id of the file to fetch
        :return: name, content
        """
        self.cursor.execute("""
                            SELECT id, filename, createdon, modifiedon FROM files
                            WHERE userid=? AND isdeleted = 0
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def get_all_data_files(self, userid):
        """
        Get all data files for that user
        :param fileid: id of the file to fetch
        :return: name, content
        """
        self.cursor.execute("""
                            SELECT id, filename, createdon, modifiedon FROM datafile
                            WHERE userid=? AND isdeleted = 0
                            """, (userid,))
        all_rows = self.cursor.fetchall()
        return all_rows

    def delete_file(self, fileid):
        """
        Delete file by file id
        :param fileid:
        :return:
        """
        deletedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE files SET isdeleted=1, deletedon = ?
                            WHERE id =? AND isdeleted = 0
                            """, (deletedon, fileid))

        rowcount = self.cursor.rowcount
        self.conn.commit()

        return rowcount

    def delete_data_file(self, fileid):
        """
        Delete data file by file id
        :param fileid:
        :return:
        """
        deletedon = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE datafile SET isdeleted=1, deletedon = ?
                            WHERE id =? AND isdeleted = 0
                            """, (deletedon, fileid))

        rowcount = self.cursor.rowcount
        self.conn.commit()

        return rowcount

    def insert_executed_file(self, fileid, args, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        createdon = datetime.datetime.now()
        self.cursor.execute("""
                            INSERT INTO execution(fileid, arguments, filecontent, outputlog,
                            print_output, sessionid, userid, createdon)
                            VALUES(?, (SELECT content FROM files WHERE id=?),?,?,?,?,?,?)
                            """, (fileid, args, fileid, output_log, output_print, sessionid, userid, createdon))

        rowid = self.cursor.lastrowid
        self.conn.commit()
        return rowid

    def update_executed_file(self, fileid, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE execution SET outputlog = ?,
                            print_output = ?,
                            modifiedon = ?
                            WHERE fileid = ? AND userid = ? AND sessionid = ?
                            """, (output_log, output_print, timenow, fileid, userid, sessionid))

        rowcount = self.cursor.rowcount
        self.conn.commit()

        return rowcount

    def update_executed_file_wsession(self, execid, output_log, output_print, sessionid, userid):
        """
        Save executed file and output
        :param fileid:
        :param output_log:
        :param output_print:
        :param userid:
        :return:
        """
        timenow = datetime.datetime.now()
        self.cursor.execute("""
                            UPDATE execution SET outputlog = ?,
                            print_output = ?, sessionid = ?,
                            modifiedon = ?
                            WHERE id = ?
                            """, (output_log, output_print, sessionid, timenow, execid))

        rowcount = self.cursor.rowcount
        self.conn.commit()

        return rowcount



    def get_execution_details(self, fileid):
        """
        Get Execution history of a file
        :param fileid:
        :return:
        """
        self.cursor.execute("""
                            SELECT id,
                                   filecontent,
                                   input_files,
                                   output_files,
                                   outputlog,
                                   print_output,
                                   userid,
                                   createdon,
                                   modifiedon
                            FROM execution
                            WHERE fileid = ?;
                            """, (fileid,))
        all_rows = self.cursor.fetchall()
        return all_rows