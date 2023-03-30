from pyspark import SparkContext, SparkConf, SQLContext
import pyodbc
import pandas as pd
from Base import DB_Connection 

appName = "PySpark SQL Server Example - via ODBC"
#master = "local"
#conf = SparkConf() \
#    .setAppName(appName) \
#    .setMaster(master) 
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
#spark = sqlContext.sparkSession

#database = "DE_Study"
table = "dbo.Test"
#user = "sa"
#password  = "Nguyenquangn01"

#conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER=localhost,1433;DATABASE={database};UID={user};PWD={password}')
conn = DB_Connection.Conn_SQLServer()

query = f"SELECT * FROM {table}"
pdf = pd.read_sql(query, conn)

df = pd.DataFrame(pdf)
##print(df.loc[0:2])
for x in range(len(df)):
        print(df.at[x,"ID"])
##sparkDF =  spark.createDataFrame(pdf)
##sparkDF.collect()
conn.close()
