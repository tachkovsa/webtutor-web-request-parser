from os import listdir, stat
from os.path import isfile, join, exists
import configparser
import time
import datetime
import sqlite3
import re
import pyodbc

# Function read configuration file
def read_config():
    __config = configparser.ConfigParser()
    __config.read("config.ini")

    return __config

# Function connect to database relates to provider config property
def connect_to_db():
    __provider = config.get("DATABASE", "Provider")

    if not __provider:
        print("Provider param is missing or empty in config file")
        exit()
    if __provider == "sqllite":
        __db_path = config.get("DATABASE", "DbPath")
        if not __db_path:
            raise Exception("sqllite: DbPath is missing or empty in config file")

        try:
            __connection = sqlite3.connect(__db_path)
        except Exception as e:
            print("sqllite: Connection failed cause: " + str(e))
            exit()
    if __provider == "mssql":
        __connection_string = config.get("DATABASE", "ConnectionString")
        __connection_string = re.sub("^\"|\"$", "", __connection_string.strip())

        if not __connection_string:
            raise Exception("mssql: Connection string is missing or empty in config file")

        try:
            __connection = pyodbc.connect(__connection_string)
        except Exception as e:
            print("mssql: Connection failed cause: " + str(e))
            exit()

    print(f"""{__provider}: Connection successfully established""")

    return __connection

# Function get log files array
def get_files(logs_dir_path):
    if not logs_dir_path:
        raise Exception("Directory path is missing or empty")

    try:
        __only_files = [f for f in listdir(logs_dir_path) if isfile(join(logs_dir_path, f)) and re.search("\.log$", f)]
        __files_count = len(__only_files)
        if __files_count == 0:
            raise Exception("Directory does not contain log files")

        print(f"""{__files_count} log files was found""")

        return __only_files

    except Exception as e:
        print("Something went wrong while reading directory: " + str(e))
        exit()

# Function parse log file and call db insertion function
def parse_file(file_path):
    try:
        with open(file_path, 'r') as __fileobject:
            for __line in __fileobject:
                if __line[0][:1] == "#":
                    continue
                
                __splitted_line = __line.strip().split(' ')

                __date = __splitted_line[0]
                __timestamp = __splitted_line[1]
                __ip = __splitted_line[2]
                __login = __splitted_line[3]
                __host = __splitted_line[4]
                __method = __splitted_line[6]
                __url = __splitted_line[7]
                __user_agent = __splitted_line[13]

                # Find mode
                __re_mode = re.search("mode=([a-zA-Z_]*)|_wt\/([a-zA-Z_]*)", __url)
                if __re_mode:
                    __mode = __re_mode[1] or __re_mode[2] or "NULL"
                else:
                    __mode = "NULL"

                # Find object_id
                __re_object_id = re.search("object_id=(\d*)|_wt\/[a-zA-Z_]*\/(\d*)|_wt\/(\d*)", __url)
                if __re_object_id:
                    __object_id = __re_object_id[1] or __re_object_id[2] or __re_object_id[3] or "NULL"
                else:
                    __object_id = "NULL"

                # Find doc_id
                __re_doc_id = re.search("doc_id=(\d*)|doc_id\/(\d*)", __url)
                if __re_doc_id:
                    __doc_id = __re_doc_id[1] or __re_doc_id[2] or "NULL"
                else:
                    __doc_id = "NULL"

                # Insert data into db
                insert_to_db(__date, __timestamp, __ip, __login, __host, __method, __url, __user_agent, __mode, __object_id, __doc_id)

    except Exception as e:
        print(f"""Unable to open file \"{file_path}\" cause: {str(e)}""")
        return False

    return True

# Insert data to database
def insert_to_db(date, timestamp, ip, login, host, method, url, user_agent, mode, object_id, doc_id):
    __provider = config.get("DATABASE", "Provider")

    try:
        if __provider == "sqllite":
            db_cursor.execute(f'''INSERT INTO {db_table_name} VALUES (
                            \'{str(date) + 'T' + str(timestamp)}\',
                            \'{str(ip)}\',
                            \'{str(login)}\',
                            \'{str(host)}\',
                            \'{str(method)}\',
                            \'{str(url)}\',
                            \'{str(user_agent)}\',
                            \'{str(mode)}\',
                            {object_id},
                            {doc_id}
                            );''')
            db_connection.commit()
        
        if __provider == "mssql":
             # datetime.datetime.strptime(str(date) + 'T' + str(timestamp)[:-], '%Y-%m-%dT%H:%M:%S%z')
            __datetime = str(date) + 'T' + str(timestamp)

            db_cursor.execute(f'''INSERT INTO {db_table_name} VALUES (
                            CAST(\'{str(__datetime)}\' AS datetime2),
                            \'{str(ip)}\',
                            \'{str(login)}\',
                            \'{str(host)}\',
                            \'{str(method)}\',
                            \'{str(url)}\',
                            \'{str(user_agent)}\',
                            \'{str(mode)}\',
                            {object_id},
                            {doc_id}
                            );''')
            db_connection.commit()

        return True
    except Exception as e:
        print("Something went wrong while call execution: " + str(e))
        return False

# Function create table in database
def create_table(table_name):
    __provider = config.get("DATABASE", "Provider")

    try:
        if __provider == "sqllite":
            db_cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp datetime,
                    ip text,
                    login text,
                    host text,
                    method text,
                    url text,
                    user_agent text,
                    mode text,
                    object_id bigint,
                    doc_id bigint
                );
            ''')
            db_connection.commit()

        if __provider == "mssql":
            db_cursor.execute(f'''
                IF NOT EXISTS (SELECT * FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))

                BEGIN
                CREATE TABLE [dbo].[{table_name}](
                    timestamp datetime,
                    ip text,
                    login text,
                    host text,
                    method text,
                    url text,
                    user_agent text,
                    mode text,
                    object_id bigint,
                    doc_id bigint
                ) 
                END
            ''')
            db_connection.commit()

    except Exception as e:
        print(str(__provider) + ": Something went wront while creating table: " + str(e))
        return False

    return True

def drop_table(table_name):
    __provider = config.get("DATABASE", "Provider")

    try:
        if __provider == "sqllite":
            db_cursor.execute(f'''DROP TABLE IF EXISTS {table_name};''')
            db_connection.commit()
        
        if __provider == "mssql":
            db_cursor.execute(f'''
                IF EXISTS (SELECT * FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))

                BEGIN
                DROP TABLE [dbo].[{table_name}]
                END
            ''')
            db_connection.commit()
        
    except Exception as e:
        print(str(__provider) + ": Something went wront while deleting table: " + str(e))
        return False

    return True


# Function close db connection and print log
def close_connection():
    db_connection.close()
    print("Connection closed")

if __name__ == "__main__":
    # Read configuration file
    try:
        config = read_config()
    except Exception as e:
        print(f"""Something went wrong while initializing config file: {e}""")
        exit()
    
    # Check logs directory path
    logs_directory_path = config.get("DEFAULT", "LogsDirectoryPath").strip()
    if not logs_directory_path:
        print("LogsDirectoryPath parameter is missing or empty in config file")
        exit()

    if not exists(logs_directory_path):
        print(f"""\"{logs_directory_path}\" does not exist""")
        exit()
    
    # Connection to database
    db_connection = connect_to_db()
    db_cursor = db_connection.cursor()
    db_table_name = config.get("DATABASE", "TableName").strip()
    db_drop_if_exists = config.get("DATABASE", "DropIfExists") == '1'

    # Prepare database
    if db_drop_if_exists:
        drop_table(db_table_name)

    if not create_table(db_table_name):
        print(f"""Unable to create table""")
        close_connection()
        exit()

    # Harvest log files
    log_files = get_files(logs_directory_path)
    log_files_count = len(log_files)

    # Walk on log files
    success_count = 0
    error_count = 0

    cur_file_index = 0
    for __log_file_name in log_files:
        cur_file_index += 1

        print(f"""Processing {cur_file_index} of {log_files_count} files...""")

        # Construct file path
        __file_path = join(logs_directory_path, __log_file_name)

        # Open file
        __is_successfull = parse_file(__file_path)
        if __is_successfull:
            success_count += 1
        else:
            error_count += 1

    print(f"""Processed {cur_file_index} of {log_files_count} files:\n{success_count} successfully,\n{error_count} unsuccessfully""")

    close_connection()
