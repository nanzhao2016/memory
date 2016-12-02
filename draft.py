"""
chcp 65001
set PYTHONIOENCODING=utf-8
"""


from pyspark import SparkConf, SparkContext, RDD, SQLContext
from pyspark.sql import Row
import sys, json, os, calendar
from pyspark.sql.types import LongType, DateType, TimestampType, IntegerType, StringType
from pyspark.sql.functions import udf, col
from datetime import datetime

sc = SparkContext("local", "MyApp")
sqlContext = SQLContext(sc)

path_ = 'C:/Users/OPEN/Documents/NanZHAO/Formation_BigData/Memoires/'
data_origin = 'velib-stations_v2.json'
data_parquet = 'tmp/db/velib.data'
data_format_update = 'tmp/db/velib.data.format.update3'
table_country = 'tmp/db/table_country.csv'
table_country_company = 'tmp/db/table_country_company.csv'
table_country_company_contract = 'tmp/db/table_country_company_contract.csv'
table_country_company_contract_name = 'tmp/db/table_country_company_contract_name.csv'
table_country_company_contract_name_bonus = 'tmp/db/table_country_company_contract_name_bonus.csv'
table_country_company_contract_name_bonus_banking = 'tmp/db/table_country_company_contract_name_bonus_banking.csv'
table_country_company_contract_name_bonus_banking_status = 'tmp/db/table_country_company_contract_name_bonus_banking_status.csv'
table_country_company_contract_name_bonus_banking_status_bikestands = 'tmp/db/table_country_company_contract_name_bonus_banking_status_bikestands.csv'


class DataBasicFunctions(object) :
	def __init__(self, path_, dataName):
		self.p = path_
		self.origin = dataName
	
	def loadOriginalData(self, read, write):
		data = sqlContext.read.json(os.path.join(self.p, read))
		data.count()
		data.write.parquet(os.path.join(self.p, write))
	
	def loadJsonData(self, read):
		return(sqlContext.read.json(os.path.join(self.p, read)))
	
	def loadParquetData(self, read):
		return(sqlContext.read.parquet(os.path.join(self.p, read)))
		
	def writeParquet(self, df, write):
		df.write.parquet(os.path.join(self.p, write))
	
	def getSchema_parquet(self, read):
		df = sqlContext.read.parquet(os.path.join(self.p, read))
		print(df.printSchema())
		
	def updateDataFormat(self, df, droplist):
		df = df.select([column for column in df.columns if column not in droplist]) 
	
		#df = df.withColumn('last_update', col('last_update')['$numberLong'].cast(LongType()))
		#udf_date = udf(lambda x: datetime.fromtimestamp(x/1e3), TimestampType())
		#df = df.withColumn('last_update', udf_date(col('last_update')))
	
		udf_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'), TimestampType())
		df = df.withColumn('timestamp', udf_date(col('collect_time')))
	
		udf_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'), DateType())
		df = df.withColumn('date', udf_date(col('collect_time'))).drop('collect_time')	
	
		udf_day = udf(lambda x: x.weekday(), IntegerType())
		df = df.withColumn('day_dig', udf_day(col('date')))
	
		udf_day = udf(lambda x: calendar.day_name[x.weekday()], StringType())
		df = df.withColumn('day', udf_day(col('date')))
		
		udf_company = udf(lambda x: x.replace('VÃ©loCitÃ©', 'VéloCité').replace('Le vÃ©lo', 'Le vélo').replace('VÃ©lo\'V', 'Vélo\'V').replace('vÃ©lOstan\'lib', 'vélOstan\'lib').replace('VÃ©lÃ´', 'Vélô').replace('GÃ¶teborg', 'Göteborg'), StringType())
		df = df.withColumn('company', udf_company(col('company')))
		
		return(df)	

#step 1
def saveVelibSourceData(dataBasicFunctionsObject):
	dataBasicFunctionsObject.loadParquetData(data_parquet)
	print("Saved in", data_parquet, "\n")

#step 2
def updateFormatVelibData(dataBasicFunctionsObject):
	data = dataBasicFunctionsObject.loadParquetData(data_parquet)
	columns = len(data.columns)
	print (columns)
	
	data = dataBasicFunctionsObject.updateDataFormat(data, ['_id', 'position', 'last_update'])
	
	columns = len(data.columns)
	print (columns)
	print(data.printSchema())
	print(data.show(2))

	dataBasicFunctionsObject.writeParquet(data, data_format_update)
	print("Saved in", data_format_update, "\n")
	
#step 3
def analyseDescription(dataBasicFunctionsObject):
	data = dataBasicFunctionsObject.loadParquetData(data_format_update)
	#subset = data.select("banking", "bonus", "company",  "contract_name", "country", "name")
	#country = subset.select("country").distinct().toPandas()['country'].values.tolist()
	#v1 = subset.distinct()
	var = data.select("country").groupby(data.country).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company").groupby(data.country, data.company).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name").groupby(data.country, data.contract_name, data.company).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name").groupby(data.country, data.contract_name, data.name, data.company).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name").groupby(data.country, data.contract_name, data.name, data.company).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus").groupby(data.country, data.contract_name, data.name, data.company, data.bonus).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking", "status").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking, data.status).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking_status), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking", "status", "bike_stands").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking, data.status, data.bike_stands).count()
	var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking_status_bikestands), sep=';', header=True, index=False, encoding = 'utf-8')
	#number = v2.count()
	#print(v2.orderBy("country").show(number))
	print("Saved in tables:", table_country, table_country_company, table_country_company_contract, table_country_company_contract_name, table_country_company_contract_name_bonus, table_country_company_contract_name_bonus_banking, table_country_company_contract_name_bonus_banking_status, table_country_company_contract_name_bonus_banking_status_bikestands, "\n")

def getSchemaInformation(dataBasicFunctionsObject, read):
	dataBasicFunctionsObject.getSchema_parquet(read)
	
def main(avg):
	velib = DataBasicFunctions(path_, data_origin)
	command = avg
	if avg == "1":
		print("\nSave the original data on spark with the format parquet.\n")
		saveVelibSourceData(velib)
	elif avg == "2":
		print("\nUpdate the data columns format and save on spark with the format parquet.\n")
		updateFormatVelibData(velib)
	elif avg == "3":
		print("\nAnalyse over the data features.\n")
		analyseDescription(velib)
	elif avg == "4":
		print("\nShow schema information.\n")
		getSchemaInformation(velib, data_format_update)	
	else:
		print("\nProcess does not exist, programme has stopped, please try again. \n")
		print("Available process: \n")
		print("1. Save original data to parquet\n")
		print("2. Update the format for the original data\n")
		print("3. Basical descriptive analysis\n")
		print("4. Schema information\n")
	

if __name__ == "__main__":
	main(sys.argv[1])