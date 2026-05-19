"""
The pipeline: read both sources, clean them into a common shape, resolve
which records are the same person, reconcile each person to one row, and
write the golden record
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import config
import utils

# --------------------------------------------------------------------------
# Cleaning: bring both sources into one common shape
# --------------------------------------------------------------------------
import sys, os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


COMMON_COLUMNS = [
    "source",  # "crm" or "transaction"
    "source_id",  # original customer_id/transaction_id for traceability
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "country",
    "registration_date",
    "purchase_date",
    "record_updated_at",  # "recency"
    "email_clean",
    "phone_match_key",
    "name_key",
]

def clean_crm(df: DataFrame) -> DataFrame:
    """Clean the CRM source and project it onto the common schema"""
    return df.select(
        F.lit("crm").alias("source"),
        F.col("customer_id").alias("source_id"),
        utils.clean_name(F.col("first_name")).alias("first_name"),
        utils.clean_name(F.col("last_name")).alias("last_name"),
        F.col("email").alias("email"),
        utils.clean_phone(F.col("phone")).alias("phone"),
        utils.blank_to_null(F.col("address")).alias("address"),
        utils.blank_to_null(F.col("city")).alias("city"),
        utils.blank_to_null(F.col("country")).alias("country"),
        utils.parse_date(F.col("registration_date")).alias(
            "registration_date"
        ),
        F.lit(None).cast("string").alias("purchase_date"),
        utils.parse_date(F.col("last_updated")).alias(
            "record_updated_at"
        ),
        F.col("email"),
        F.col("phone"),
         F.concat_ws("|",F.col("first_name"),F.col("last_name")).alias(
            "name_key"
        ),
    )


def run() -> None:
    """build the golden record and write it out"""
    spark = utils.create_spark_session("CRM_SYSTEM")
    df = utils.read_csv(spark,config.DATA_DIR/"crm_customers.csv",config.CRM_SCHEMA)
    cleaned_df = clean_crm(df)
    cleaned_df.show(20)


if __name__ == "__main__":
    run()
