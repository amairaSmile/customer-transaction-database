from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
import config

# create spark session
def create_spark_session(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark
# read files
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

def clean_name(col: Column) -> Column:
    """Trim whitespace on a name part"""
    return blank_to_null(F.regexp_replace(F.trim(col), r"\s+", " "))

def blank_to_null(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "",None).otherwise(trimmed)

def clean_email(col: Column) -> Column:
    return blank_to_null(F.trim(col))

def phone_match_key(col: Column) -> Column:
    # match on digits because CRM has +33/+44 and txn doesn't
    digits = F.regexp_replace(col,r"\D","")
    n = config.PHONE_NUM_LEN
    return F.when(F.length(digits) >= n, F.substring(digits,-n,n)).otherwise(None)