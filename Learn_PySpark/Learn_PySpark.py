from pyspark import SparkContext
from pyspark.sql import SparkSession

#create SparkSession
spark = SparkSession.builder\
                    .appName("Test_JDBC")\
                    .config("spark.driver.extraClassPath","sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre11.jar") \
                    .getOrCreate()

#.config("spark.jars", "mssql-jdbc-12.2.0.jre11.jar") \
#config("spark.driver.extraClassPath","sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar")
SERVER_ADDR = "NHATNQ12"
server_name = "jdbc:sqlserver://{SERVER_ADDR}"
database_name = "DE_Study"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "table_name"
username = "sa"
password = "Nguyenquangn01" # Please specify password here


# Read from SQL Table
df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", url) \
  .option("dbtable", "select * from OrderTbl") \
  .option("user", username) \
  .option("password", password) \
  .load()

df.show()