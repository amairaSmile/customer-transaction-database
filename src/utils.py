from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from . import config

# create spark session
def create_spark_session(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
def read_csv(spark,path: str,schema: str):
    df = spark.read.option("header", True)\
        .schema(schema)\
        .csv(str(path))
    return df

def clean_phone(col: Column)-> Column:
    digits = F.regexp_replace(col,r"\D","")
    return F.when(F.length(digits)==0,None).otherwise(digits)
def parse_date(col: Column)-> Column:
    trimmed= F.trim(col)
    parsed = F.lit(None).cast("date")
    for fmt in config.DATE_FORMAT:
        parsed = F.coalesce(parsed,F.try_to_date(trimmed,fmt))
    return F.date_format(parsed,"yyyy-MM-dd")