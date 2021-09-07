from os import listdir
from os.path import isfile, join
import sqlite3
import re

db_path = './db/example.db';

print(f"""Connecting to \"{db_path}\" sqlite database...""")
# Connecting to db
try:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    print("Connection established")
except Exception as e:
    print("Connection failed cause: " + e)
    exit()

# Delete table if exists
cur.execute('''DROP TABLE IF EXISTS web_requests;''')
# Create table if not exists
cur.execute('''CREATE TABLE IF NOT EXISTS web_requests (
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
                );''')

logs_path = './Logs'
print(f"""Looking for log files in \"{logs_path}\" directory""")

log_files_count = 0
try:
    only_files = [f for f in listdir(logs_path) if isfile(join(logs_path, f))]
    log_files_count = len(only_files)

    print(f"""{log_files_count} log files was found""")
except Exception as e:
    print("Something went wrong while reading directory:" + e)
    exit()

cur_file_index = 0
for file_name in only_files:
    count = 0
    cur_file_index += 1

    print(f"""Processing {cur_file_index} of {log_files_count} files...""")

    # Construct file path
    file_path = f'''./Logs/{file_name}'''

    # Open file
    try:
        log_file = open(file_path, 'r')
        Lines = log_file.readlines()
    except Exception as e:
        print(f"""Unable to open file \"{file_path}\" cause: {e}""")
        continue

    for line in Lines:
        count += 1
        if count < 5:
            continue

        splitted_line = line.strip().split(' ')

        date = splitted_line[0]
        time = splitted_line[1]
        ip = splitted_line[2]
        login = splitted_line[3]
        host = splitted_line[4]
        # -
        method = splitted_line[6]
        url = splitted_line[7]
        # - - - - - 
        user_agent = splitted_line[13]

        # Find mode
        re_mode = re.search("mode=([a-zA-Z_]*)|_wt\/([a-zA-Z_]*)", url)
        if re_mode:
            mode = re_mode[1] or re_mode[2] or "NULL"
        else:
            mode = "NULL"

        # Find object_id
        re_object_id = re.search("object_id=(\d*)|_wt\/[a-zA-Z_]*\/(\d*)|_wt\/(\d*)", url)
        if re_object_id:
            object_id = re_object_id[1] or re_object_id[2] or re_object_id[3] or "NULL"
        else:
            object_id = "NULL"

        # Find doc_id
        re_doc_id = re.search("doc_id=(\d*)|doc_id\/(\d*)", url)
        if re_doc_id:
            doc_id = re_doc_id[1] or re_doc_id[2] or "NULL"
        else:
            doc_id = "NULL"

        # Insert data into db
        cur.execute(f'''INSERT INTO web_requests VALUES (
                        \'{str(date) + 'T' + str(time)}\',
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
        con.commit()

# Close db connection
con.close()
print("Connection closed...")

