# Must be installed on Linux host
# sudo yum -y install python-devel
# curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
# sudo python get-pip.py
# sudo pip install six
# sudo pip install bit_array
# sudo pip install thrift
# sudo pip install impyla
import datetime
from impala.dbapi import connect
conn = connect(host='sf-mngdn2', port=21050)
table_cursor = conn.cursor()
table_cursor.execute('show tables')
#************** CREATE DATABASE parquet_backup ********************
create_command = "create table parquet_backup."
drop_command = "DROP TABLE IF EXISTS parquet_backup."
insert_command = "INSERT INTO TABLE parquet_backup."
#"INSERT INTO TABLE some_parquet_table SELECT * FROM kudu_table"

#results = cursor.fetchall()
for table in table_cursor:
    print(table[0])
    drop_command = drop_command + table[0]
    create_command = create_command + table[0] + " ("
    insert_command = insert_command + table[0] + " SELECT * FROM " + table[0]
    col_cursor = conn.cursor()
    col_cursor.execute('describe test1')
    for col_names in col_cursor:
        #print col_names[0] + " " + col_names[1]
        create_command = create_command + col_names[0] + " " + col_names[1] + ","
    col_cursor.close()

    create_command = create_command[:-1] + ") STORED AS PARQUET"
    execute_command = conn.cursor()
    print drop_command
    print datetime.datetime.now()
    execute_command.execute(drop_command)
    drop_command = "DROP TABLE IF EXISTS parquet_backup."
    print create_command
    print datetime.datetime.now()
    execute_command.execute(create_command)
    create_command = "create table parquet_backup."
    print insert_command
    print datetime.datetime.now()
    execute_command.execute(insert_command)
    insert_command = "INSERT INTO TABLE parquet_backup."
    print "End of run for " + table[0] + " "+ str(datetime.datetime.now())
    execute_command.close()

table_cursor.close()
conn.close()

end = "end"


# create_table = conn.cursor()
# create_table.execute('CREATE TABLE parquet_backup.test3 (id BIGINT,name STRING,PRIMARY KEY(id)) PARTITION BY HASH (id) PARTITIONS 3 STORED AS KUDU')