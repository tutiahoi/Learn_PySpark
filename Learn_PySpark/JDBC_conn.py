from pyspark import SparkContext, SparkConf, SQLContext

appName = "PySpark SQL Server Example - via JDBC"
master = "local"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath","sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession
#demo 2023
serverName = "localhost"
database = "DE_Study"
table = "dbo.OrderTbl"
user = "sa"
password  = "Nguyenquangn01"
url = "jdbc:sqlserver://" +serverName + ":1433;DatabaseName=" + database + ";encrypt=true;trustServerCertificate=true;"
jdbcDF = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "OrderTbl") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

jdbcDF.show()
