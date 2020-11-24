import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

ARN = config.get("IAM_ROLE","ARN")

# Drop
population_table_drop = "DROP TABLE IF EXISTS population;"
country_table_drop = "DROP TABLE IF EXISTS country;"
visa_table_drop = "DROP TABLE IF EXISTS visa;"
airportus_table_drop = "DROP TABLE IF EXISTS airportus;"
time_table_drop = "DROP TABLE IF EXISTS time;"
immigration_table_drop = "DROP TABLE IF EXISTS immigration;"
staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_immigration;"


# Create
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  "start_time" BIGINT PRIMARY KEY,
  "day" INT NOT NULL,
  "week" INT NOT NULL,
  "month" INT NOT NULL,
  "year" INT NOT NULL,
  "weekday" INT NOT NULL
);""")


staging_immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_immigration (
  "cicid" BIGINT not null,
  "i94yr" INT NULL,
  "i94mon" INT NULL,
  "i94cit" INT NULL,
  "i94res" INT NULL,
  "i94port" VARCHAR(6) NOT NULL,
  "arrdate" INT NOT NULL,
  "i94mode" INT NULL,
  "i94addr" VARCHAR(60) NULL,
  "depdate" INT NULL,
  "i94bir" INT NULL,
  "i94visa" INT NOT NULL,
  "count" INT NULL,
  "dtadfile" VARCHAR(20) NULL,
  "visapost" VARCHAR(60) NULL,
  "entdepa" VARCHAR(8) NULL,
  "entdepd" VARCHAR(8) NULL,
  "matflag" VARCHAR(8) NULL,
  "biryear" INT NULL,
  "dtaddto" VARCHAR(8) NULL,
  "gender" VARCHAR(1) NULL,
  "airline" VARCHAR(8) NULL,
  "admnum" BIGINT NULL,
  "fltno" VARCHAR(8) NULL,
  "visatype" VARCHAR(4) NULL,
  "start_time" BIGINT NOT NULL
);""")


immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS immigration (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "cicid" BIGINT not null,
  "i94yr" INT NULL,
  "i94mon" INT NULL,
  "i94cit" INT NULL,
  "i94res" INT NULL,
  "i94port" VARCHAR(6) NOT NULL,
  "arrdate" INT NOT NULL,
  "i94mode" INT NULL,
  "i94addr" VARCHAR(60) NULL,
  "depdate" INT NULL,
  "i94bir" INT NULL,
  "i94visa" INT NOT NULL,
  "count" INT NULL,
  "dtadfile" VARCHAR(20) NULL,
  "visapost" VARCHAR(60) NULL,
  "entdepa" VARCHAR(8) NULL,
  "entdepd" VARCHAR(8) NULL,
  "matflag" VARCHAR(8) NULL,
  "biryear" INT NULL,
  "dtaddto" VARCHAR(8) NULL,
  "gender" VARCHAR(1) NULL,
  "airline" VARCHAR(8) NULL,
  "admnum" BIGINT NULL,
  "fltno" VARCHAR(8) NULL,
  "visatype" VARCHAR(4) NULL,
  "start_time" BIGINT NOT NULL
);""")


airportus_table_create = ("""
CREATE TABLE IF NOT EXISTS airportus (
  "iata_code" VARCHAR(6) PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL,
  "type" VARCHAR(60) NOT NULL,
  "elevation_ft" NUMERIC NULL,
  "iso_country" VARCHAR(10) NOT NULL,
  "iso_region" VARCHAR(20) NOT NULL,
  "municipality" VARCHAR(120) NOT NULL,
  "gps_code" VARCHAR(60) NULL,
  "local_code" VARCHAR(60) NULL,
  "coordinates" VARCHAR(80) NOT NULL
);""")

visa_table_create = ("""
CREATE TABLE IF NOT EXISTS visa (
  "id" INT IDENTITY(0,1) PRIMARY KEY,
  "code" INT NOT NULL,
  "description" VARCHAR(60) NOT NULL
);""")

country_table_create = ("""
CREATE TABLE IF NOT EXISTS country (
  "id" INT IDENTITY(0,1) PRIMARY KEY,
  "code" INT NOT NULL,
  "country" VARCHAR(255) NOT NULL
);""")

population_table_create = ("""
CREATE TABLE IF NOT EXISTS population (
  "id" INT IDENTITY(0,1) PRIMARY KEY,
  "stateCode" VARCHAR(255) NOT NULL,
  "city" VARCHAR(255) NOT NULL,
  "state" VARCHAR(255) NOT NULL,
  "medianAge" NUMERIC NOT NULL,
  "malePopulation" BIGINT  NOT NULL,
  "femalePopulation" BIGINT NOT NULL,
  "totalPopulation" BIGINT  NOT NULL,
  "numberofVeterans" BIGINT NULL,
  "foreignBorn" BIGINT NULL,
  "averageHouseholdSize" NUMERIC NULL
);""")




# QUERY LISTS
create_table_queries = [time_table_create,immigration_table_create,airportus_table_create,visa_table_create,country_table_create,population_table_create, staging_immigration_table_create]
drop_table_queries = [population_table_drop ,country_table_drop,visa_table_drop,airportus_table_drop,time_table_drop,immigration_table_drop,staging_immigration_table_drop]
