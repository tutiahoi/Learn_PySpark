from Base import DB_Actions
import pandas as pd


strsql ="Insert into dbo.Test(ID,Code,Description) values({},{},{})"
strsql = strsql.format("5","'SSI'","'SSI'")
result = DB_Actions.Wirte_DB_SQLServer(strsql)
print(result)
