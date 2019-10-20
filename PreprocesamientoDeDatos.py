#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('1st_exercise').getOrCreate()
df = spark.read.option("inferSchema", "true").csv("data_raw.csv", header=True).cache()

df1 = df.select(F.avg('trip_distance'))

df1.show()


# In[5]:


keys = ['***************', '***************', '***************', 
        '***************', '***************']
years = [2013, 2014, 2015, 2016, 2017]

for key, year in zip(keys, years):
    path = './data/weather_' + str(year) + '.csv'
    if os.path.isfile(path):
        continue
    df, _ = query_weather(key=key, year=year, state='IL', area='Chicago')
    df.to_csv(path, index=False)
    print('File saved:\t', path)


# In[ ]:


if not os.path.isfile('./data/data_raw.csv'):
    # año 2013
    # cargar información del clima
    weather_2013 = pd.read_csv('./data/weather_2013.csv', parse_dates=['date'])
    trip_2013 = pd.read_csv('./data/2013/Divvy_Trips_2013.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2013 = pd.read_csv('./data/2013/Divvy_Stations_2013.csv')

    # fusionar información
    merged_2013 = merge(trip_2013, station_2013, weather_2013)

    # año 2014, Q1 y Q2
    # load weather information
    weather_2014 = pd.read_csv('./data/weather_2014.csv', parse_dates=['date'])

    trip_2014_Q1Q2 = pd.read_csv('./data/2014_Q1Q2/Divvy_Trips_2014_Q1Q2.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2014_Q1Q2 = pd.read_excel('./data/2014_Q1Q2/Divvy_Stations_2014-Q1Q2.xlsx')

    # fusionar información
    merged_2014_Q1Q2 = merge(trip_2014_Q1Q2, station_2014_Q1Q2, weather_2014)

    # año 2014, Q3 y Q4
    trip_2014_Q3_07 = pd.read_csv('./data/2014_Q3Q4/Divvy_Trips_2014-Q3-07.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2014_Q3_0809 = pd.read_csv('./data/2014_Q3Q4/Divvy_Trips_2014-Q3-0809.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2014_Q4 = pd.read_csv('./data/2014_Q3Q4/Divvy_Trips_2014-Q4.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2014_Q3Q4 = pd.read_csv('./data/2014_Q3Q4/Divvy_Stations_2014-Q3Q4.csv')

    # fusionar información
    merged_2014_Q3_07 = merge(trip_2014_Q3_07, station_2014_Q3Q4, weather_2014)
    merged_2014_Q3_0809 = merge(trip_2014_Q3_0809, station_2014_Q3Q4, weather_2014)
    merged_2014_Q4 = merge(trip_2014_Q4, station_2014_Q3Q4, weather_2014)

    # año 2015, Q1 y Q2
    # cargar información del clima
    weather_2015 = pd.read_csv('./data/weather_2015.csv', parse_dates=['date'])

    trip_2015_Q1 = pd.read_csv('./data/2015_Q1Q2/Divvy_Trips_2015-Q1.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2015_Q2 = pd.read_csv('./data/2015_Q1Q2/Divvy_Trips_2015-Q2.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2015 = pd.read_csv('./data/2015_Q1Q2/Divvy_Stations_2015.csv')

    # fusionar información
    merged_2015_Q1 = merge(trip_2015_Q1, station_2015, weather_2015)
    merged_2015_Q2 = merge(trip_2015_Q2, station_2015, weather_2015)

    # año 2015, Q3 y Q4
    trip_2015_Q3_07 = pd.read_csv('./data/2015_Q3Q4/Divvy_Trips_2015_07.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2015_Q3_08 = pd.read_csv('./data/2015_Q3Q4/Divvy_Trips_2015_08.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2015_Q3_09 = pd.read_csv('./data/2015_Q3Q4/Divvy_Trips_2015_09.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2015_Q4 = pd.read_csv('./data/2015_Q3Q4/Divvy_Trips_2015_Q4.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])

    # fusionar información
    merged_2015_Q3_07 = merge(trip_2015_Q3_07, station_2015, weather_2015)
    merged_2015_Q3_08 = merge(trip_2015_Q3_08, station_2015, weather_2015)
    merged_2015_Q3_09 = merge(trip_2015_Q3_09, station_2015, weather_2015)
    merged_2015_Q4 = merge(trip_2015_Q4, station_2015, weather_2015)

    # año 2016, Q1 y Q2
    # cargar información del clima
    weather_2016 = pd.read_csv('./data/weather_2016.csv', parse_dates=['date'])

    trip_2016_Q1 = pd.read_csv('./data/2016_Q1Q2/Divvy_Trips_2016_Q1.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2016_Q2_04 = pd.read_csv('./data/2016_Q1Q2/Divvy_Trips_2016_04.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2016_Q2_05 = pd.read_csv('./data/2016_Q1Q2/Divvy_Trips_2016_05.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    trip_2016_Q2_06 = pd.read_csv('./data/2016_Q1Q2/Divvy_Trips_2016_06.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2016_Q1Q2 = pd.read_csv('./data/2016_Q1Q2/Divvy_Stations_2016_Q1Q2.csv')

    # fusionar información
    merged_2016_Q1 = merge(trip_2016_Q1, station_2016_Q1Q2, weather_2016)
    merged_2016_Q2_04 = merge(trip_2016_Q2_04, station_2016_Q1Q2, weather_2016)
    merged_2016_Q2_05 = merge(trip_2016_Q2_05, station_2016_Q1Q2, weather_2016)
    merged_2016_Q2_06 = merge(trip_2016_Q2_06, station_2016_Q1Q2, weather_2016)

    # año 2016, Q3 y Q4
    trip_2016_Q3 = pd.read_csv('./data/2016_Q3Q4/Divvy_Trips_2016_Q3.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2016_Q3 = pd.read_csv('./data/2016_Q3Q4/Divvy_Stations_2016_Q3.csv')

    trip_2016_Q4 = pd.read_csv('./data/2016_Q3Q4/Divvy_Trips_2016_Q4.csv', low_memory=False, 
                            parse_dates=['starttime', 'stoptime'])
    station_2016_Q4 = pd.read_csv('./data/2016_Q3Q4/Divvy_Stations_2016_Q4.csv')

    # fusionar información
    merged_2016_Q3 = merge(trip_2016_Q3, station_2016_Q3, weather_2016)
    merged_2016_Q4 = merge(trip_2016_Q4, station_2016_Q4, weather_2016)

    # año 2017, Q1 y Q2
    # cargar información del clima
    weather_2017 = pd.read_csv('./data/weather_2017.csv', parse_dates=['date'])

    trip_2017_Q1 = pd.read_csv('./data/2017_Q1Q2/Divvy_Trips_2017_Q1.csv', low_memory=False, 
                            parse_dates=['start_time', 'end_time'])
    trip_2017_Q1.rename(columns={'start_time': 'starttime', 'end_time': 'stoptime'}, inplace=True)

    trip_2017_Q2 = pd.read_csv('./data/2017_Q1Q2/Divvy_Trips_2017_Q2.csv', low_memory=False, 
                            parse_dates=['start_time', 'end_time'])
    trip_2017_Q2.rename(columns={'start_time': 'starttime', 'end_time': 'stoptime'}, inplace=True)

    station_2017_Q1Q2 = pd.read_csv('./data/2017_Q1Q2/Divvy_Stations_2017_Q1Q2.csv')

    # fusionar información
    merged_2017_Q1 = merge(trip_2017_Q1, station_2017_Q1Q2, weather_2017)
    merged_2017_Q2 = merge(trip_2017_Q2, station_2017_Q1Q2, weather_2017)

    # año 2017, Q3 y Q4
    trip_2017_Q3 = pd.read_csv('./data/2017_Q3Q4/Divvy_Trips_2017_Q3.csv', low_memory=False, 
                            parse_dates=['start_time', 'end_time'])
    trip_2017_Q3.rename(columns={'start_time': 'starttime', 'end_time': 'stoptime'}, inplace=True)

    trip_2017_Q4 = pd.read_csv('./data/2017_Q3Q4/Divvy_Trips_2017_Q4.csv', low_memory=False, 
                            parse_dates=['start_time', 'end_time'])
    trip_2017_Q4.rename(columns={'start_time': 'starttime', 'end_time': 'stoptime'}, inplace=True)

    station_2017_Q3Q4 = pd.read_csv('./data/2017_Q3Q4/Divvy_Stations_2017_Q3Q4.csv')

    # fusionar información
    merged_2017_Q3 = merge(trip_2017_Q3, station_2017_Q3Q4, weather_2017)
    merged_2017_Q4 = merge(trip_2017_Q4, station_2017_Q3Q4, weather_2017)
    
    # concatenar y guardar los datos combinados
    objs = [merged_2013, merged_2014_Q1Q2, merged_2014_Q3_07, merged_2014_Q3_0809, merged_2014_Q4, 
            merged_2015_Q1, merged_2015_Q2, merged_2015_Q3_07, merged_2015_Q3_08, merged_2015_Q3_09, 
            merged_2015_Q4, merged_2016_Q1, merged_2016_Q2_04, merged_2016_Q2_05, merged_2016_Q2_06, 
            merged_2016_Q3, merged_2016_Q4, merged_2017_Q1, merged_2017_Q2, merged_2017_Q3, merged_2017_Q4]
    data = pd.concat(objs, axis=0)
    data.to_csv('./data/data_raw.csv', index=False)
    _ = gc.collect()

