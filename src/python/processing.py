"""
The pipeline: read both sources, clean them into a common shape, resolve
which records are the same person, reconcile each person to one row, and
write the golden record
"""

from pyspark.sql import DataFrame,Window
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

# Identify assign one person_id across both systems

def generate_identity(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates() # there are multiple exact row in CRM file
    """ 
    Primary key: email 
    if email not present then form a key using phone_num and name ,at last if no phn number ,fill in with person_id
    """
    df = df.withColumn("identity_key",F.when(F.col("email").isNotNull(),F.concat(F.lit("e:"),F.col("email")))
                       .when(F.col("phone_match_key").isNotNull() & F.col("full_name").isNotNull(),F.concat(F.lit("pn:"), F.col("phone_match_key"),
                        F.lit("|"), F.lower(F.col("full_name")))).otherwise(F.concat(F.lit("s:"), F.col("source"), F.col("source_id")))
                       )

    return df.withColumnRenamed("identity_key","person_id")

def reconcile(df: DataFrame)-> DataFrame:
    crm_first = Window.partitionBy("person_id").orderBy(
        F.when(F.col("source") == "crm", 0).otherwise(1),
        F.col("record_updated_at").desc_nulls_last(),
    )
    # most recent handles people who moved,contact details updated
    most_recent = Window.partitionBy("person_id").orderBy(
        F.col("record_updated_at").desc_nulls_last()
    )
    pick_crm = lambda field: F.first(F.col(field), ignorenulls=True).over(crm_first).alias(field)
    pick_recent = lambda field: F.first(F.col(field), ignorenulls=True).over(most_recent).alias(field)

    return df.select(
        "person_id",
        pick_crm("first_name"),
        pick_crm("last_name"),
        pick_crm("full_name"),
        pick_crm("registration_date"),
        pick_recent("address"),
        pick_recent("city"),
        pick_recent("country"),
        pick_recent("email"),
        pick_recent("phone"),
    ).dropDuplicates(["person_id"])

def write_golden(df: DataFrame) -> None:
    config.OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
    path = str(config.OUTPUT_DIR /"golden_customers.csv")
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(path)
    print(f"Golden record written: {path}")

def run() -> None:
    """build the golden record and write it out"""
    try:
        spark = utils.create_spark_session("CRM_SYSTEM")
        crm_df = utils.read_csv(spark, config.DATA_DIR / "crm_customers.csv", config.CRM_SCHEMA)
        trans_df = utils.read_csv(spark, config.DATA_DIR / "transaction_customers.csv", config.TRANSACTION_SCHEMA)
        crm_cleaned_df = clean_crm(crm_df)
        trans_cleaned_df = clean_transactions(trans_df)
        combined_df = crm_cleaned_df.unionByName(trans_cleaned_df)
        final_df= generate_identity(combined_df)
        #print("input rows:", combined_df.count())
        #print("output rows:", final_df.count())
        golden_df = reconcile(final_df)
        write_golden(golden_df)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()




if __name__ == "__main__":
    run()
