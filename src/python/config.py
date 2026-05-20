from pathlib import Path
from pyspark.sql.types import StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT/"data"
OUTPUT_DIR = PROJECT_ROOT/"output"
IDENTY_FIELD = ["email","phone"]
CRM_IDENTY_FIELD = ["first_name","last_name","registration_date"]
ADDRESS_FIELD= ["address","city","country"]
DATE_FORMAT= ['yyyy-MM-dd','yyyyMMdd']
PHONE_NUM_LEN= 9
# define the schema for the csv

CRM_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("last_updated", StringType(), True),
    ]
)
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("purchase_date", StringType(), True),
    ]
)