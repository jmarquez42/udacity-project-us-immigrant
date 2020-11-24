from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (LoadDimensionOperator, StageToRedshiftOperator, 
LoadFactOperator, DataQualityOperator)



from pyspark.sql import SparkSession
import logging, time, os, configparser
from datetime import datetime, timedelta
from pyspark.sql.types import TimestampType, StringType, IntegerType
from pyspark.sql.functions import udf, col, monotonically_increasing_id, when, count, col, isnull
from pyspark.sql.functions import hour, dayofmonth, dayofweek, month, year, weekofyear, from_unixtime, to_timestamp, date_format

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config = configparser.ConfigParser()
config.read(base_dir + r'/dl.cfg')

WORKSPACE = config['LOCAL']['PATH_WORKSPACE']
DATA = config['LOCAL']['PATH_SAS']
BKS3A = config['S3']['BUCKET_S3A']
BKS3 = config['S3']['BUCKET_S3']

@udf(IntegerType())
def convertTime(dateString):
    if dateString is not None:
        return int(time.mktime((datetime(1960, 1, 1).date() + timedelta(dateString)).timetuple()))
    else:
        return None
    
get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)), TimestampType())


def readFilesCSV(file, **kwargs):
    import pandas as pd
    df = pd.read_csv(file, **kwargs)
    return df

def cleanerAirportus(**kwargs):
    #'/home/user/workspace/static_data/COUNTRIES.csv'
    df_airportus = readFilesCSV( kwargs['fileCsv'], encoding="ISO-8859-1")
    df_airportus = df_airportus.drop(['continent','ident'],axis=1).loc[(df_airportus.type.isin(['small_airport','medium_airport','large_airport'])
                                                                        & df_airportus.iso_country.isin(['US'])), ['iata_code', 'name', 'type',
                                                                                                                   'elevation_ft','iso_country','iso_region',
                                                                                                                   'municipality', 'gps_code','local_code',
                                                                                                                   'coordinates']].dropna(how='all',subset=['iata_code'])
    return {'data': df_airportus.values, 'count': df_airportus.shape[0]}


def cleanerPopulation(**kwargs):
    #'/home/user/workspace/static_data/COUNTRIES.csv'
    df_population = readFilesCSV( kwargs['fileCsv'], sep=';', encoding="ISO-8859-1")
    df_population = df_population.loc[(~df_population['Male Population'].isnull() | ~df_population['Female Population'].isnull()),
                                            ['State Code','City','State','Median Age','Male Population','Female Population',
                                             'Total Population','Number of Veterans','Foreign-born','Average Household Size']] \
    .drop_duplicates().fillna(value={'Number of Veterans': 0, 'Foreign-born': 0, 'Average Household Size': 0}).copy()
    return {'data': df_population.values, 'count': df_population.shape[0]}


def cleanerCountry(**kwargs):
    import csv
    df_country = readFilesCSV( kwargs['fileCsv'], sep=';',quoting=csv.QUOTE_NONE)
    return {'data': df_country.values, 'count': df_country.shape[0]}

def cleanerVisa(**kwargs):
    import csv
    df_country = readFilesCSV( kwargs['fileCsv'], sep=';',quoting=csv.QUOTE_NONE)
    return {'data': df_country.values, 'count': df_country.shape[0]}
    
def cleannerImmigration(**kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    os.environ["AWS_DEFAULT_REGION"] = 'us-west-2'
    os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
    os.environ['AWS_SECRET_ACCESS_KEY']= credentials.secret_key
    spark = SparkSession.builder.\
    config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.1.0-s_2.11').\
    enableHiveSupport().getOrCreate()
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(kwargs['fileSAS'])
    df_spark.write.parquet("/tmp/sas_data",mode='overwrite')
    df_spark = spark.read.parquet("/tmp/sas_data")
    df_spark = df_spark.na.drop("all", subset=["i94addr"]).drop("occup","entdepu","insnum") \
           .withColumn("start_time", convertTime(df_spark.arrdate))
    df_spark.selectExpr('cast(cicid AS int) as cicid', ' cast(i94yr AS int) as i94yr',' cast(i94mon AS int) as i94mon','cast(i94cit AS int) as i94cit','cast(i94res AS int) as i94res','i94port',
                'cast(arrdate AS int) as arrdate','cast(i94mode AS int)i94mode','i94addr','cast(depdate AS int) as depdate','cast(i94bir AS int) as i94bir','cast(i94visa AS int) as i94visa',
                'cast(count AS int) as count','dtadfile','visapost','entdepa', 'entdepd','matflag','cast(cicid AS int) biryear','dtaddto','gender','airline','cast(admnum AS int) as admnum','fltno',
                'visatype','start_time').write.parquet("/tmp/immigration_data",mode='overwrite')
    df_immigration = spark.read.parquet('/tmp/immigration_data')
    df_immigration.write.parquet(f'{kwargs["s3"]}' + "immigration",mode='overwrite')
    nroRegister = df_immigration.count()
    spark.stop()
    return {'count': nroRegister}
    
def cleannerTime(**kwargs):
    
    spark = SparkSession.builder.\
    config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.1.0-s_2.11').\
    enableHiveSupport().getOrCreate()
    df_time = spark.read.parquet(kwargs['fileParquet']).withColumn("start_time_date", get_timestamp(col("start_time"))) \
                .select("start_time","start_time_date") \
                .withColumn("day",dayofmonth(col("start_time_date"))) \
                .withColumn("week",weekofyear(col("start_time_date"))) \
                .withColumn("month",month(col("start_time_date"))) \
                .withColumn("year",year(col("start_time_date"))) \
                .withColumn("weekday",date_format(col("start_time_date"), 'u')) \
                .selectExpr('cast(start_time as string) as start_time', 'cast(day as string) as day', 'cast(week as string) AS week', 'cast(month as string) AS month', 'cast(year as string) AS year', 'cast(weekday as string) AS weekday ').distinct().toPandas()
    spark.stop()
    return {'data': df_time.values, 'count': df_time.shape[0]}


dag = DAG('etl_immigration_udacity',
      description='ETL for the 2016 immigration data',
      start_date = datetime(2020, 11, 21),
      schedule_interval="@once",
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


cleaner_immigration = PythonOperator(task_id='cleaner_immigration', python_callable=cleannerImmigration, 
                                 op_kwargs={'fileSAS': f'{DATA}/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat','s3':BKS3A})

cleaner_time = PythonOperator(task_id='cleaner_time', python_callable=cleannerTime, 
                                 op_kwargs={'fileParquet': '/tmp/immigration_data'})

cleaner_visa = PythonOperator(task_id='cleaner_visa', python_callable=cleanerVisa, 
                                 op_kwargs={'fileCsv': f'{WORKSPACE}/static_data/VISA.csv'})
cleaner_country = PythonOperator(task_id='cleaner_country', python_callable=cleanerCountry, 
                                 op_kwargs={'fileCsv': f'{WORKSPACE}/static_data/COUNTRIES.csv'})
cleaner_airportus = PythonOperator(task_id='cleaner_airportus', python_callable=cleanerAirportus, 
                                 op_kwargs={'fileCsv': f'{WORKSPACE}/airport-codes_csv.csv'})
cleaner_population = PythonOperator(task_id='cleaner_population', python_callable=cleanerPopulation, 
                                 op_kwargs={'fileCsv': f'{WORKSPACE}/us-cities-demographics.csv'})


#load
stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='stage_immigration',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region = 'us-west-2',
    table="public.staging_immigration",
    s3_bucket=f"{BKS3}immigration/",
)


load_immigration_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="""public.immigration (cicid,i94yr,i94mon,i94cit,i94res,i94port,arrdate,
    i94mode,i94addr,depdate,i94bir,i94visa,"count",dtadfile,visapost,entdepa,
    entdepd,matflag,biryear,dtaddto,gender,airline,admnum,fltno,visatype,start_time)""",
    insertTable ="""SELECT cicid,i94yr,i94mon,i94cit,i94res,i94port,arrdate,
    i94mode,i94addr,depdate,i94bir,i94visa,"count",dtadfile,visapost,entdepa,
    entdepd,matflag,biryear,dtaddto,gender,airline,admnum,fltno,visatype,start_time 
    FROM staging_immigration""",
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.time",
    insertTable = """ INSERT INTO "time" (start_time, "day", week, "month", "year", weekday)
                      VALUES(%s, %s, %s, %s, %s, %s);""",
    taskId = "cleaner_time",
    deleteLoad = True,
    provide_context=True,
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id='Load_visa_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.visa",
    insertTable = """ INSERT INTO visa (code, description) VALUES(%s, %s);""",
    taskId = "cleaner_visa",
    deleteLoad = True,
    provide_context=True,
)

load_country_dimension_table = LoadDimensionOperator(
    task_id='Load_country_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.country",
    insertTable = """INSERT INTO country (code, country) VALUES(%s, %s);""",
    taskId = "cleaner_country",
    deleteLoad = True,
    provide_context=True,
)

load_airportus_dimension_table = LoadDimensionOperator(
    task_id='Load_airportus_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.airportus",
    insertTable = """INSERT INTO airportus (iata_code, name, "type", elevation_ft, iso_country, iso_region,
    municipality, gps_code, local_code, coordinates) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
    taskId = "cleaner_airportus",
    deleteLoad = True,
    provide_context=True,
)

load_population_dimension_table = LoadDimensionOperator(
    task_id='Load_population_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.population",
    insertTable = """INSERT INTO population (statecode, city, state, medianage, malepopulation, 
    femalepopulation, totalpopulation, numberofveterans, foreignborn, averagehouseholdsize) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
    taskId = "cleaner_population",
    deleteLoad = True,
    provide_context=True,
)

run_quality_checks_airportus = DataQualityOperator(
    task_id='Run_data_quality_checks_airportus',
    dag=dag,
    field="iata_code",
    table='public.airportus',
    redshift_conn_id="redshift",
)


run_quality_checks_population = DataQualityOperator(
    task_id='Run_data_quality_checks_population',
    dag=dag,
    field="statecode",
    table='public.population',
    redshift_conn_id="redshift",
)


run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    field="start_time",
    table='public.time',
    redshift_conn_id="redshift",
)

run_quality_checks_immigration = DataQualityOperator(
    task_id='Run_data_quality_checks_immigration',
    dag=dag,
    field="cicid",
    table='public.immigration',
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> cleaner_immigration
start_operator >> cleaner_country
start_operator >> cleaner_visa
start_operator >> cleaner_airportus
start_operator >> cleaner_population
cleaner_immigration >> stage_immigration_to_redshift
cleaner_immigration >> cleaner_time
cleaner_visa >> load_visa_dimension_table 
cleaner_country >> load_country_dimension_table 
cleaner_airportus >> load_airportus_dimension_table 
cleaner_population >> load_population_dimension_table 
cleaner_time >> load_time_dimension_table
stage_immigration_to_redshift >> load_immigration_table
load_time_dimension_table >> run_quality_checks_time
load_immigration_table >> run_quality_checks_immigration
load_population_dimension_table >> run_quality_checks_population
load_airportus_dimension_table >> run_quality_checks_airportus
run_quality_checks_immigration >> end_operator
run_quality_checks_time >> end_operator
run_quality_checks_population >> end_operator
run_quality_checks_airportus >> end_operator
load_visa_dimension_table >> end_operator
load_country_dimension_table >> end_operator