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
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable # I had a firewall issue so had to add it's optional


COMMON_COLUMNS = [
    "source",  # crm/transaction
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
    "record_updated_at",  # recency
    "email_clean",
    "phone_match_key",
    "full_name",
]

def clean_crm(df: DataFrame) -> DataFrame:
    """Clean the CRM source and project it onto the common schema"""
    return df.select(
        F.lit("crm").alias("source"),
        F.col("customer_id").alias("source_id"),
        utils.clean_name(F.col("first_name")).alias("first_name"),
        utils.clean_name(F.col("last_name")).alias("last_name"),
        utils.clean_email(F.col("email")).alias("email"),
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
        utils.phone_match_key(F.col("phone")).alias("phone_match_key"),
        F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias(
            "full_name"
        ),
    )


def clean_transactions(df: DataFrame) -> DataFrame:
    """Clean the transaction source and project it onto the common schema.
     one person can have many rows the most recent purchase
     is the latest data
    """
    return df.select(
        F.lit("transaction").alias("source"),
        F.col("transaction_id").alias("source_id"),
        utils.clean_name(F.col("first_name")).alias("first_name"),
        utils.clean_name(F.col("last_name")).alias("last_name"),
        utils.clean_email(F.col("customer_email")).alias("email"),
        utils.clean_phone(F.col("phone")).alias("phone"),
        utils.blank_to_null(F.col("shipping_address")).alias("address"),
        utils.blank_to_null(F.col("city")).alias("city"),
        utils.blank_to_null(F.col("country")).alias("country"),
        F.lit(None).cast("string").alias("registration_date"),
        utils.parse_date(F.col("purchase_date")).alias("purchase_date"),
        utils.parse_date(F.col("purchase_date")).alias(
            "record_updated_at"
        ),
        utils.phone_match_key(F.col("phone")).alias("phone_match_key"),
        F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias(
            "full_name"
        ),
    )
def run() -> None:
    """build the golden record and write it out"""
    spark = utils.create_spark_session("CRM_SYSTEM")
    crm_df = utils.read_csv(spark,config.DATA_DIR/"crm_customers.csv",config.CRM_SCHEMA)
    trans_df = utils.read_csv(spark,config.DATA_DIR/"transaction_customers.csv",config.TRANSACTION_SCHEMA)
    crm_cleaned_df = clean_crm(crm_df)
    trans_cleaned_df = clean_transactions(trans_df)
    crm_cleaned_df.show(20)
    trans_cleaned_df.show(20)


if __name__ == "__main__":
    run()
