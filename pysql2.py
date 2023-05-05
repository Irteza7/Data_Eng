import ibm_db
import ibm_db_dbi
import ibm_db_sa
import sqlalchemy
import pandas as pd


dsn_hostname = "764264db-9824-4b7c-82df-40d1b13897c2.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" # e.g.: "54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
dsn_uid = "ynr01638"        # e.g. "abc12345"
dsn_pwd = "IXOaPrYHTMmngIQ2"      # e.g. "7dBZ3wWt9XN6$o0J"

dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"            # e.g. "BLUDB"
dsn_port = "32536"                # e.g. "32733" 
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              #i.e. "SSL"


#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

#print the connection string to check correct values are specified
print(dsn)



#Create database connection

try:
    engine = sqlalchemy.create_engine(f"ibm_db_sa://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}")
    print("Connected to database:", dsn_database, "as user:", dsn_uid, "on host:", dsn_hostname)
except Exception as e:
    print("Unable to connect:", str(e))



chicago_socioeconomic_data = pd.read_csv('https://data.cityofchicago.org/resource/jcxq-k9xf.csv')
# chicago_socioeconomic_data.to_sql('chicago_socioeconomic_data', engine, if_exists='replace', index=False)
data_to_push = chicago_socioeconomic_data.head(5)
chicago_socioeconomic_data.shape
data_to_push.to_sql('did_i_get_in',engine,if_exists='append')

engine.dispose