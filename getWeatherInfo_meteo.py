from pyspark import SparkContext, SQLContext, RDD
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType, TimestampType, DoubleType
from datetime import datetime
import os

sc = SparkContext("local", "MyApp")
sqlContext = SQLContext(sc)

#path = '../data/meteo/'
path_input = 'C:/Users/OPEN/Documents/NanZHAO/Formation_BigData/Memoires/meteo/'
path_output = 'C:/Users/OPEN/Documents/NanZHAO/Formation_BigData/Memoires/tmp/db/'
keeplist = ['date', 'ff', 't', 'u', 'rr1', 'rr3', 'rr6', 'rr12', 'rr24']
strlist = ['ff', 'rr1', 'rr3', 'rr6', 'rr12', 'rr24']

def readCsvFile(input):
	mm = sc.textFile(input)
	mm.count()
	header = mm.first()
	mm = mm.filter(lambda line: line != header).persist()
	mm = mm.map(lambda line: line.split(";"))
	header = header.split(";")
	df = sqlContext.createDataFrame(mm, header)
	return(df)


def generateDataFrame(input):
	readCsvFile(input)
	df = df.filter(df.numer_sta == '07149')
	return (df)

def cleanDataFrame(df):
	getDate_udf_date = udf(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S'), DateType())
	getDate_udf_time = udf(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S'), TimestampType())
	
	df_sub = df.select([column for column in df.columns if column in keeplist])
	df_sub = df_sub.withColumn('timestamp', getDate_udf_time(col('date')))
	df_sub = df_sub.withColumn('date', getDate_udf_date(col('date')))
	df_sub = df_sub.withColumn('temperature', col('t').cast(DoubleType())-273.15).drop('t')
	df_sub = df_sub.withColumn('humidite', col('u').cast(DoubleType())/100).drop('u')
	for column in strlist:
		df_sub = df_sub.withColumn(column, df_sub[column].cast('double'))

	print(df_sub.printSchema())
	print("\n")
	return(df_sub)
	
def summariseGroup(df):
	group = df.groupBy(df['date'])
	summary = group.avg('ff','temperature', 'humidite', 'rr1', 'rr3', 'rr6', 'rr12', 'rr24')
	summary = summary.withColumnRenamed('avg(ff)', 'avg_ff').withColumnRenamed('avg(temperature)', 'avg_temperature').withColumnRenamed('avg(humidite)', 'avg_humidite').withColumnRenamed( 'avg(rr1)','avg_rr1').withColumnRenamed('avg(rr3)','avg_rr3').withColumnRenamed('avg(rr6)', 'avg_rr6').withColumnRenamed('avg(rr12)', 'avg_rr12').withColumnRenamed('avg(rr24)', 'avg_rr24')
	return(summary)
	
def chargeWeatherData(filelist):	
	for file in filelist:
			print(file)
			print("\n")
			df = generateDataFrame(os.path.join(path_input, file))
			df_sub = cleanDataFrame(df)
			output = file.replace("csv", "parquet")
			df_sub.write.parquet(os.path.join(path_output, output))
			print(file, 'write to parquet')
			print("\n")
			summary = summariseGroup(df_sub)
			print(summary.show(31))
			output = file.replace("csv", "parquet.sum")
			summary.write.parquet(os.path.join(path_output, output))
			print(file, 'write to parquet.sum')
			print("\n")
			print("="*50)
			print("\n")

def combineDataFrame():
	m1 = sqlContext.read.parquet(os.path.join(path_output, 'meteo_08.parquet.sum'))
	m2 = sqlContext.read.parquet(os.path.join(path_output, 'meteo_09.parquet.sum'))
	m3 = sqlContext.read.parquet(os.path.join(path_output, 'meteo_10.parquet.sum'))
	m = m1.unionAll(m2)
	m = m.unionAll(m3)
	m.write.parquet(os.path.join(path_output, 'meteo.sum.parquet'))
	
def meteo1_meteo2(input):
	meteo2 = readCsvFile(input)
	getDate_udf_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
	meteo2 = meteo2.withColumn('date', getDate_udf_date(col('date')))
	toDoule = ['tempMax', 'tempMin', 'precipitation']
	for column in toDoule:
		meteo2 = meteo2.withColumn(column, meteo2[column].cast('double'))
	#print(meteo2.printSchema())
	#print(meteo2.show(10))
	
	meteo1 = sqlContext.read.parquet(os.path.join(path_output, 'meteo.sum.parquet'))
	meteo_mix = meteo1.join(meteo2, meteo1.date == meteo2.date).drop(meteo2.date)
	
	#print(meteo_mix.printSchema())
	#print(meteo_mix.show(10))
	#print(meteo_mix.count())
	#meteo_mix.write.parquet(os.path.join(path_output, 'meteo_mix.parquet'))
	meteo_mix_short = meteo_mix.drop('avg_rr1').drop('avg_rr3').drop('avg_rr6').drop('avg_rr12').drop('avg_rr24')
	print(meteo_mix_short.show(10))
	meteo_mix_short.write.parquet(os.path.join(path_output, 'meteo_mix_short.parquet'))
	
def main():
	files = ['meteo_08.csv', 'meteo_09.csv', 'meteo_10.csv']
	#files = ['meteo_08.csv']
	#chargeWeatherData(files)
	#combineDataFrame()
	meteo1_meteo2(os.path.join(path_output, 'meteo2.csv'))
		
	

if __name__ == "__main__": 
	main()