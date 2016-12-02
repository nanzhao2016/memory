from lxml import html
import requests, pandas, os
from datetime import datetime, timedelta

linklist = ['https://www.wunderground.com/history/airport/LFPO/2016/8/29/MonthlyCalendar.html?req_city=Paris&req_state=&req_statename=France&reqdb.zip=00000&reqdb.magic=1&reqdb.wmo=07149', 'https://www.wunderground.com/history/airport/LFPO/2016/9/29/MonthlyCalendar.html?req_city=Paris&req_state=&req_statename=France&reqdb.zip=00000&reqdb.magic=1&reqdb.wmo=07149', 'https://www.wunderground.com/history/airport/LFPO/2016/10/29/MonthlyCalendar.html?req_city=Paris&req_state=&req_statename=France&reqdb.zip=00000&reqdb.magic=1&reqdb.wmo=07149']
#link = 'https://www.wunderground.com/history/airport/LFPO/2016/8/29/MonthlyCalendar.html?req_city=Paris&req_state=&req_statename=France&reqdb.zip=00000&reqdb.magic=1&reqdb.wmo=07149'
path_output = 'C:/Users/OPEN/Documents/NanZHAO/Formation_BigData/Memoires/tmp/db/'

def pageParser(tree):
	weather = tree.xpath('.//td[@class="show-for-large-up"]/text()')
	temperature_max = tree.xpath('.//td[@class="values highLow"][@colspan="2"]/span[@class="high"]/span[@class="wx-value"]/text()')
	temperature_max = temperature_max[0::2]
	temperature_min = tree.xpath('.//td[@class="values highLow"][@colspan="2"]/span[@class="low"]/span[@class="wx-value"]/text()')
	temperature_min = temperature_min[0::2]
	precipitation = tree.xpath('.//td[@class="values precip"][@colspan="2"]/span[@class="wx-value"]/text()')
	precipitation = precipitation[0::2]
	
	df_weather = pandas.DataFrame(weather, columns = ['weather'])
	df_weather = df_weather[['weather']].applymap(lambda x: x.split("\n  ")[1])
	
	df_temperature_max = pandas.DataFrame(temperature_max, columns=['tempMax'])
	df_temperature_max = df_temperature_max[['tempMax']].applymap(lambda x: x.replace('°', ''))
	df_temperature_max[['tempMax']] = df_temperature_max[['tempMax']].apply(pandas.to_numeric)
	
	df_temperature_min = pandas.DataFrame(temperature_min, columns=['tempMin'])
	df_temperature_min = df_temperature_min[['tempMin']].applymap(lambda x: x.replace('°', ''))
	df_temperature_min[['tempMin']] = df_temperature_min[['tempMin']].apply(pandas.to_numeric)
	
	df_precipitation = pandas.DataFrame(precipitation, columns=['precipitation'])
	df_precipitation = df_precipitation[['precipitation']].applymap(lambda x: x.split('\n\t\t')[0].split('\n')[1].replace(' ', ''))
	df_precipitation[['precipitation']] = df_precipitation[['precipitation']].apply(pandas.to_numeric)
	
	df = pandas.concat([df_weather, df_temperature_max, df_temperature_min, df_precipitation], axis=1)
	return(df)
	
def generateTimeSeries(start, end):
	step = timedelta(days=1)
	date = []
	while start < end:
		date.append(start.strftime('%Y-%m-%d'))
		start += step
	
	return (pandas.DataFrame(date, columns=['date']))
	
def main():
	df = pandas.DataFrame()
	for link in linklist:	
		page = requests.get(link)
		tree = html.fromstring(page.content)
		meteo = pageParser(tree)
		df = pandas.concat([df, meteo])
	df.index = list(range(0, 92))
	date = generateTimeSeries(datetime(2016, 8, 1), datetime(2016, 11, 1))
	#print(type(date))
	#print(df)
	results = pandas.concat([date, df], axis=1)
	print(results)
	results.to_csv(os.path.join(path_output, 'meteo2.csv'), sep=';', header=True, index=False)

if __name__ == "__main__": 
	main()