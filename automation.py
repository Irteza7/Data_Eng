# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to DB2
import ibm_db

# Connect to MySQL
# connect to database
connection = mysql.connector.connect(user='root', password='abcd!',host='172.17.0.2',database='sales')
# create cursor
cursor = connection.cursor()

# Connect to DB2

# connectction details

dsn_hostname = "0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "cbj01037"        # e.g. "abc12345"
dsn_pwd = "abcd"      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "31198"                # e.g. "50000" 
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# Find out the last rowid from DB2 data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.

def get_last_rowid():
	SQL = "SELECT MAX(ROWID) FROM sales_data"
	stmt = ibm_db.exec_immediate(conn, SQL)
	tuple = ibm_db.fetch_tuple(stmt)
	return tuple[0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than 
# the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = "SELECT * FROM sales_data WHERE rowid > %s"
    row_list = []
    cursor.execute(SQL, (rowid,))

    for row in cursor.fetchall():
        row_list.append(row)
    return row_list


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records):
	insert_query = """
	INSERT INTO sales_data (ROWID, PRODUCT_ID, CUSTOMER_ID, QUANITITY)
	VALUES (?, ?, ?, ?)
	"""
	try:
		cursor.executemany(insert_query, conn)
		print("Data inserted successfully")
	except Exception as e:
		print("Exception occurred: ", e)
        

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
# close connection
connection.close()
# disconnect from DB2 data warehouse
# close connection
ibm_db.close(conn)
# End of program
