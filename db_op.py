import cx_Oracle
import re

# Mock pipeline engine api to allow testing outside pipeline engine
try:
    api
except NameError:
    class api:
        @staticmethod
        def send(port, data):
             print(port + ":\n" + str(data))

        @staticmethod   
        def set_port_callback(port, callback):
             print(
                 "Call \"" + callback.__name__ + "\" to simulate behavior when messages arrive at port \"" + port + "\".")
             #--Test query
             #callback(f"select * FROM {tblName}")
             
             #--Test initial loading
            #  data = [
            #     {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试11","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"0","DI_TRANSACTION_TS":"2020-12-10 20:03:56.623000000","ID1":"1","ID2":"1","NAME1":"test11"}},
            #     {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试12","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"1","DI_TRANSACTION_TS":"2020-12-10 12:06:57.690000000","ID1":"1","ID2":"2","NAME1":"test12"}},
            #     {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试21","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"2","DI_TRANSACTION_TS":"2020-12-10 12:07:57.430000000","ID1":"2","ID2":"1","NAME1":"test21"}},
            #     {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试22","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"3","DI_TRANSACTION_TS":"2020-12-10 12:11:12.249000000","ID1":"2","ID2":"2","NAME1":"test22"}},
            #     {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试31","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"4","DI_TRANSACTION_TS":"2020-12-10 12:11:12.264000000","ID1":"3","ID2":"1","NAME1":"test31"}},
            #     {"attributes":{"message.lastLine":True},  "body":{"DESCRIPTION":"测试32","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"5","DI_TRANSACTION_TS":"2020-12-10 12:11:12.278000000","ID1":"3","ID2":"2","NAME1":"test32"}}
            #  ]
            #  for msg in data:
            #    callback(msg)
             
             #--Test insert
             #data = {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试41","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"8","DI_TRANSACTION_TS":"2020-12-10 12:19:19.357000000","ID1":"4","ID2":"1","NAME1":"test41"}}
             #callback(data)

             #--Test full
             data = [
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试11","DI_OPERATION_TYPE":"B","DI_SEQUENCE_NUMBER":"6","DI_TRANSACTION_TS":"2020-12-10 12:16:28.187000000","ID1":"1","ID2":"1","NAME1":"test11"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"update","DI_OPERATION_TYPE":"U","DI_SEQUENCE_NUMBER":"6","DI_TRANSACTION_TS":"2020-12-10 12:16:28.187000000","ID1":"1","ID2":"1","NAME1":"更新"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试12","DI_OPERATION_TYPE":"B","DI_SEQUENCE_NUMBER":"7","DI_TRANSACTION_TS":"2020-12-10 12:17:50.267000000","ID1":"1","ID2":"2","NAME1":"test12"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"update","DI_OPERATION_TYPE":"U","DI_SEQUENCE_NUMBER":"7","DI_TRANSACTION_TS":"2020-12-10 12:17:50.267000000","ID1":"1","ID2":"2","NAME1":"更新"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"测试41","DI_OPERATION_TYPE":"I","DI_SEQUENCE_NUMBER":"8","DI_TRANSACTION_TS":"2020-12-10 12:19:19.357000000","ID1":"4","ID2":"1","NAME1":"test41"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"update","DI_OPERATION_TYPE":"B","DI_SEQUENCE_NUMBER":"9","DI_TRANSACTION_TS":"2020-12-10 12:20:55.577000000","ID1":"1","ID2":"2","NAME1":"更新"}},
                 {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"update12","DI_OPERATION_TYPE":"U","DI_SEQUENCE_NUMBER":"9","DI_TRANSACTION_TS":"2020-12-10 12:20:55.577000000","ID1":"1","ID2":"2","NAME1":"更新12"}},
                {"attributes":{"message.lastLine":False}, "body":{"DESCRIPTION":"update","DI_OPERATION_TYPE":"D","DI_SEQUENCE_NUMBER":"10","DI_TRANSACTION_TS":"2020-12-10 12:23:28.393000000","ID1":"1","ID2":"1","NAME1":"更新"}}
             ]
             for msg in data: callback(msg)


        class config:
            host = 'andy-VirtualBox'
            svc = 'XE'
            port = '1521'
            user = 'kitty'
            password = 'danny'
            tableName = 'Test2'

            # The below three configuration is only for initial loading.
            initialLoading = False
            dropExistingTable = True # option for dropping existing target table.
            buckInsertRowCount = 4   # initial loading use buck insert for improving performance, choose an appropriate row count for each bulk insert. 

            schema = {"ID1":"NUMBER", "ID2": "NUMBER", "NAME1":"VARCHAR2(20)", "DESCRIPTION":"VARCHAR2(100)"} # target table scheme
            primaryKeys = ["ID1", "ID2"]    # target table primary key(s)     

            # Settings for SQL SELECT statement
            delimiter = ','     # Delimiter to separate postrgres columns in output
            outInbatch = False
            outbatchsize = 2

host = api.config.host
serviceName = api.config.svc
user = api.config.user
password = api.config.password
port = api.config.port
delimiter = api.config.delimiter
outInbatch = api.config.outInbatch
outbatchsize = api.config.outbatchsize
tblName = api.config.tableName


def getUpdatedFields(data):
    updatedFields = {}
    if beforeData is not None:
        for key in tblSchema.keys():
            if key not in prkeys and beforeData[key] != data[key]: #TODO
                updatedFields[key] = data[key]
    else:
        for key in tblSchema.keys():
            if key not in prkeys:
                updatedFields[key] = data[key] # TODO
    return updatedFields


query_stmt_mapping = {"INSERT": "", "UPDATE": "", "DELETE": "", "CREATE": "", "DROP": ""}
tblSchema = api.config.schema
prkeys = api.config.primaryKeys

def getQueryStmt(opType, data = {}):
    postgres_query = query_stmt_mapping[opType]
    if len(postgres_query) == 0:
        if opType == "CREATE":
            strFields = ""
            i = 0
            for item in tblSchema.items():
                i += 1
                strFields += f"{item[0]} {item[1]}{'' if i==len(tblSchema) else ','}"           
            strPrimary =  ""
            i = 0
            for key in prkeys:
                i += 1
                strPrimary += f"{key}{'' if i==len(prkeys) else ','}"
            if len(strPrimary) == 0:
                postgres_query = f"CREATE TABLE {tblName} ({strFields});"
            else:
                postgres_query = f"CREATE TABLE {tblName} ({strFields}, CONSTRAINT CUST_PK PRIMARY KEY ({strPrimary}));"

            postgres_query = f"CREATE TABLE {tblName} ({strFields}, CONSTRAINT CUST_PK PRIMARY KEY ({strPrimary}))"
            query_stmt_mapping["CREATE"] = postgres_query
        elif opType == "DROP":
            postgres_query = f"""begin
                                     execute immediate 'drop table {tblName}';
                                     exception when others then if sqlcode <> -942 then raise; end if;
                                 end;"""
            query_stmt_mapping["DROP"] = postgres_query
        elif opType == "INSERT":
            strFields = ""
            strPlaceholder = ""
            i = 0 
            for key in tblSchema.keys():
                i += 1
                strFields += f"{key}{'' if i==len(tblSchema) else ','}"
                strPlaceholder += f":{i}{'' if i==len(tblSchema) else ','}"
            ##strPlaceholder =  "%s,"*(len(tblSchema)-1) + "%s"
            postgres_query = f"INSERT INTO {tblName} ({strFields}) VALUES ({strPlaceholder})" 
            query_stmt_mapping["INSERT"] = postgres_query
        elif opType == "UPDATE":
            updatedFields = getUpdatedFields(data)  
            strFields = ""
            i = 0 
            for key in updatedFields.keys():
                i += 1
                strFields += f"{key} = :{i}{'' if i==len(updatedFields) else ', '}"        
            strPrimary = ""
            j = 0
            for key in prkeys:
                j += 1
                i += 1
                strPrimary += f"{key} = :{i}{'' if j==len(prkeys) else ' AND '}"     
            postgres_query = f"UPDATE {tblName} SET {strFields} WHERE {strPrimary}"
            #query_stmt_mapping["UPDATE"] = postgres_query #! DO NOT UPDATE MAPPING for update operation since every update may have different changes.
        elif opType == "DELETE":
            strPrimary = ""
            i = 0
            for key in prkeys:
                i += 1
                strPrimary += f"{key} = :{i}{'' if i==len(prkeys) else ' AND '}"
            postgres_query = f"DELETE FROM {tblName} WHERE {strPrimary}" 
            query_stmt_mapping["DELETE"] = postgres_query
    return postgres_query


def handle_query(query):
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=serviceName)
    with cx_Oracle.connect(user, password, dsn_tns, encoding="UTF-8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            outStr = ""
            rows = []
            
            if not outInbatch:
                rows = cursor.fetchall()
                for r in rows:
                    for i, c in enumerate(r):
                        outStr += str(c) + \
                            ('' if i==len(r)-1 else delimiter)
                    outStr += "\n"
                api.send("output", outStr)
            else:
                rows = cursor.fetchmany(outbatchsize)
                while(rows):
                    for r in rows:
                        for i, c in enumerate(r):
                            outStr += str(c) + \
                                ('' if i==len(r)-1 else delimiter)
                        outStr += "\n"
                    api.send("output", outStr)
                    outStr = ""
                    rows = cursor.fetchmany(outbatchsize)


def createTable(connection):
    with connection.cursor() as cursor:
        create_table_query = getQueryStmt("CREATE")
        cursor.execute(create_table_query)
        connection.commit()
        api.send("debug", "Table created successfully.") 

def dropTable(connection):
    with connection.cursor() as cursor:
        drop_table_query = getQueryStmt("DROP")
        cursor.execute(drop_table_query)
        connection.commit()
        api.send("debug", "Table dropped successfully.")


def insert(postgres_insert_query, record):
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=serviceName)
    with cx_Oracle.connect(user, password, dsn_tns, encoding="UTF-8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(postgres_insert_query, record)
            connection.commit()
            count = cursor.rowcount
            api.send("debug", f"{count} Record inserted successfully.")


def bulkInsert(postgres_insert_query, records, connection):
    with connection.cursor() as cursor:
        cursor.executemany(postgres_insert_query, records)
        connection.commit()
        api.send("debug", f"{cursor.rowcount} Records inserted successfully.")


def update(postgres_update_query, record):
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=serviceName)
    with cx_Oracle.connect(user, password, dsn_tns, encoding="UTF-8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(postgres_update_query, record)
            connection.commit()
            count = cursor.rowcount
            api.send("debug", f"{count} Record Updated successfully.")


def delete(sql_delete_query, ids):
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=serviceName)
    with cx_Oracle.connect(user, password, dsn_tns, encoding="UTF-8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_delete_query, ids)
            connection.commit()
            count = cursor.rowcount
            api.send("debug", f"{count} Record deleted successfully.")


beforeData = None # For update purpose
records = []      # holding records for bulk insert
initialLoading = api.config.initialLoading
dropExistingTable = api.config.dropExistingTable
buckInsertRowCount = api.config.buckInsertRowCount
openedConnection = None

def ingest_data(data):
    global beforeData
    global dropExistingTable
    global openedConnection

    body = data["body"]
    attributes = data["attributes"]
    op = body["DI_OPERATION_TYPE"]
    if initialLoading:
        if openedConnection is None:
            dsn_tns = cx_Oracle.makedsn(host, port, service_name=serviceName)
            openedConnection = cx_Oracle.connect(user, password, dsn_tns, encoding="UTF-8")
        if dropExistingTable:
            dropTable(openedConnection)
            createTable(openedConnection)
            dropExistingTable = False
        
        flush = False
        postgres_insert_query = getQueryStmt("INSERT")
        record_to_insert = ()

        for key in tblSchema.keys():
            record_to_insert += body[key],
        records.append(record_to_insert)

        if len(records) == buckInsertRowCount or attributes["message.lastLine"]:
            flush = True

        if(flush):
            bulkInsert(postgres_insert_query, records, openedConnection)
            records.clear()

        if attributes["message.lastLine"]:
            openedConnection.close()
            openedConnection = None
    elif op == "I":
        postgres_insert_query = getQueryStmt("INSERT")
        record_to_insert = ()
        for key in tblSchema.keys():
            record_to_insert += body[key],
        insert(postgres_insert_query, record_to_insert)
    elif op == "B":
        beforeData = body.copy()
    elif op == "U":
        postgres_update_query = getQueryStmt("UPDATE", body)
        ids = ()
        fileds_to_update = ()
        updatedFields = getUpdatedFields(body)
        for key in updatedFields:
            fileds_to_update += updatedFields[key],
        for key in prkeys:
            ids += body[key],
        record = fileds_to_update + ids
        update(postgres_update_query, record)
        beforeData = None
    elif op == "D":
        postgres_delete_query = getQueryStmt("DELETE")
        ids = ()
        for key in prkeys:
            ids += body[key],
        delete(postgres_delete_query, ids)


# Interface for integrating the postgres query function into the pipeline engine
def on_sql(query):
    if query:
        m = re.match(r'\s*select', query, flags=re.IGNORECASE)
        if m:
            handle_query(query)
        else:
            api.send("debug", "Only support SELECT Statement.")
    else:
        api.send("debug", "Input is empty.")

def on_data(data):
    ingest_data(data)

# Triggers the request for every message (the message provides the query)
#api.set_port_callback("sql", on_sql)
api.set_port_callback("data", on_data)


# # Create a table

# cursor.execute("""
#     begin
#         execute immediate 'drop table todoitem';
#         exception when others then if sqlcode <> -942 then raise; end if;
#     end;""")

# cursor.execute("""
#     create table todoitem (
#         id number generated always as identity,
#         description varchar2(4000),
#         creation_ts timestamp with time zone default current_timestamp,
#         done number(1,0),
#         primary key (id))""")

# # Insert some data

# rows = [ ("Task 1", 0 ),
#          ("Task 2", 0 ),
#          ("Task 3", 1 ),
#          ("Task 4", 0 ),
#          ("Task 5", 1 ) ]

# cursor.executemany("insert into todoitem (description, done) values(:1, :2)", rows)
# print(cursor.rowcount, "Rows Inserted")

# connection.commit()

# # Now query the rows back
# for row in cursor.execute('select description, done from todoitem'):
#     if (row[1]):
#         print(row[0], "is done")
#     else:
#         print(row[0], "is NOT done")