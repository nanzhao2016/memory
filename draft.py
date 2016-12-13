"""
chcp 65001
set PYTHONIOENCODING=utf-8
"""


from pyspark import SparkConf, SparkContext, RDD, SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
import sys, json, os, calendar
from pyspark.sql.types import LongType, DateType, TimestampType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import udf, col, to_date, lit
from datetime import datetime

sc = SparkContext("local", "MyApp")
#sqlContext = SQLContext(sc)

sqlContext = SparkSession.builder \
	.master("local") \
    .appName("Bike Sharing") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()



path_ = 'C:/Users/OPEN/Documents/NanZHAO/Formation_BigData/Memoires/tmp/db/'
data_origin = 'velib-stations_v2.json'
data_parquet = 'velib.data'
data_format_update = 'velib.data.format.update3'
table_country = 'table_country.csv'
table_country_company = 'table_country_company.csv'
table_country_company_contract = 'table_country_company_contract.csv'
table_country_company_contract_name = 'table_country_company_contract_name.csv'
table_country_company_contract_name_bonus = 'table_country_company_contract_name_bonus.csv'
table_country_company_contract_name_bonus_banking = 'table_country_company_contract_name_bonus_banking.csv'
table_country_company_contract_name_bonus_banking_status = 'table_country_company_contract_name_bonus_banking_status.csv'
table_country_company_contract_name_bonus_banking_status_bikestands = 'table_country_company_contract_name_bonus_banking_status_bikestands.csv'
table_country_company_contract_name_bonus_banking_bikestands = 'table_country_company_contract_name_bonus_banking_bikestands.csv'
paris_parquet = 'all_paris'
paris_week_1 = 'paris_first_week.csv'
station_location = 'station_location.csv'

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
	
	def writeCsv(self, df, write):
		df.toPandas().to_csv(os.path.join(self.p, write), sep=';', header=True, index=False, encoding='utf-8')
	
	def getSchema_parquet(self, read):
		df = sqlContext.read.parquet(os.path.join(self.p, read))
		print(df.printSchema())
		print(df.show(10))
		
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
	
	def filterByDate(self, df, date_str):
		dates = to_date(lit(date_str)).cast(DateType())
		return (df.where(df.date == dates))
		
	def filterBetweenDates(self, df, dates):
		date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
		return (df.where((df.date > date_from)& (df.date < date_to)))
	
	def filterByContractName(self, df, name):
		return (df.filter(df.contract_name == name))

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
	dataBasicFunctionsObject.writeCsv(var, table_country)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company").groupby(data.country, data.company).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name").groupby(data.country, data.contract_name, data.company).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name").groupby(data.country, data.contract_name, data.name, data.company).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus").groupby(data.country, data.contract_name, data.name, data.company, data.bonus).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name_bonus)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name_bonus_banking)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking", "status").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking, data.status).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name_bonus_banking_status)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking_status), sep=';', header=True, index=False, encoding = 'utf-8')
	
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking", "status", "bike_stands").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking, data.status, data.bike_stands).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name_bonus_banking_status_bikestands)
	#var.toPandas().to_csv(os.path.join(dataBasicFunctionsObject.p, table_country_company_contract_name_bonus_banking_status_bikestands), sep=';', header=True, index=False, encoding = 'utf-8')
	#number = v2.count()
	#print(v2.orderBy("country").show(number))
	var = data.select("country", "company", "contract_name", "name", "bonus", "banking", "bike_stands").groupby(data.country, data.contract_name, data.name, data.company, data.bonus, data.banking, data.bike_stands).count()
	dataBasicFunctionsObject.writeCsv(var, table_country_company_contract_name_bonus_banking_bikestands)
	print("Saved in tables:", table_country, table_country_company, table_country_company_contract, table_country_company_contract_name, table_country_company_contract_name_bonus, table_country_company_contract_name_bonus_banking, table_country_company_contract_name_bonus_banking_status, table_country_company_contract_name_bonus_banking_status_bikestands, table_country_company_contract_name_bonus_banking_bikestands, "\n")

#step 4
def getSchemaInformation(dataBasicFunctionsObject, read):
	dataBasicFunctionsObject.getSchema_parquet(read)

#step 5	
def filterData(dataBasicFunctionsObject, option, conditions, read, write):
	df = dataBasicFunctionsObject.loadParquetData(read)
	if option == "contract":
		df = dataBasicFunctionsObject.filterByContractName(df, conditions)
		dataBasicFunctionsObject.writeParquet(df, write)
	elif option == "one-day":
		df = dataBasicFunctionsObject.filterByDate(df, conditions)
		dataBasicFunctionsObject.writeCsv(df, write)
	elif option == "two-day":
		df = dataBasicFunctionsObject.filterBetweenDates(df, conditions)
		dataBasicFunctionsObject.writeCsv(df, write)
		
	print(df.count())
	print(df.show(10))

#step 6	
def filterData_Paris(dataBasicFunctionsObject, read, dates, write):
	df = dataBasicFunctionsObject.loadParquetData(read)
	df = dataBasicFunctionsObject.filterBetweenDates(df, dates)
	dataBasicFunctionsObject.writeCsv(df, write)
	print(df.count())
	print(df.show(10))

#step 7	
def getLocation(dataBasicFunctionsObject, read, write):
	df = dataBasicFunctionsObject.loadParquetData(read)
	keeplist = ["banking", "bike_stands", "bonus", "contract_name", "country", "name", "number", "position"]
	df = df.select([column for column in df.columns if column in keeplist]) 
	df = df.distinct()
	"""
	print(df.printSchema())
	print(df.count())
	print(df.show(10))
	"""
	df = df.withColumn('lat', col('position')['lat'].cast(DoubleType()))
	df = df.withColumn('lng', col('position')['lng'].cast(DoubleType()))
	df = df.drop('position')
	print(df.printSchema())
	print(df.show(10))
	dataBasicFunctionsObject.writeCsv(df, write)
	
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
		data_to_read = data_parquet # data_format_update
		getSchemaInformation(velib, data_to_read)	
	elif avg == "5":
		print("\nFilter a sub-dataframe.\n")
		option = "two-day"
		conditions = ("2016-08-23", "2016-08-31")
		filter_out = "08-24-30.csv"
		filterData(velib, option, conditions, data_format_update, filter_out)
	elif avg == "6":
		print("\nFilter a week data from Paris.\n")
		dates = ("2016-08-23", "2016-08-31")
		filterData_Paris(velib, paris_parquet, dates, paris_week_1)
	elif avg == "7":
		print("\nGet geographic information.\n")
		getLocation(velib, data_parquet, station_location)	
	else:
		print("\nProcess does not exist, programme has stopped, please try again. \n")
		print("Available process: \n")
		print("1. Save original data to parquet\n")
		print("2. Update the format for the original data\n")
		print("3. Basical descriptive analysis\n")
		print("4. Schema information\n")
		print("5. Dataframe filter\n")
		print("6. Get a week information from Paris\n")
		print("7. Get georaphic information\n")
	

if __name__ == "__main__":
	main(sys.argv[1])