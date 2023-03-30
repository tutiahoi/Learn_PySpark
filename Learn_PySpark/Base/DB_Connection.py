from pyspark import SparkContext, SparkConf, SQLContext
import pyodbc


def Conn_SQLServer():
    appName = "PySpark SQL Server Example - via ODBC"
    master = "local"
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) 

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    database = "DE_Study"
    user = "sa"
    password  = "Nguyenquangn01"

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER=localhost,1433;DATABASE={database};UID={user};PWD={password}')
    
    return conn

