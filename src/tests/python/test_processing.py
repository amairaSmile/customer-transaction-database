import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2] / "python"))

import processing
COLS = processing.COMMON_COLUMNS

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.appName("tests").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()

def _row(**overrides):
    return tuple(overrides.get(c) for c in COLS)

def _df(spark, rows):
    # createDataFrame can't infer types from all-null columns.
    schema = ", ".join(f"{c} string" for c in COLS)
    return spark.createDataFrame(rows, schema)

def test_same_email_gives_same_person_id(spark):
    """The primary matching rule: same email -> same customer."""
    df = _df(spark, [
        _row(source="crm",         source_id="C1", email="a@x.com", full_name="Alice"),
        _row(source="transaction", source_id="T1", email="a@x.com", full_name="Alice"),
    ])
    out = processing.generate_identity(df).collect()
    assert out[0].person_id == out[1].person_id


def test_crm_wins_for_first_name(spark):
    """CRM is authoritative for who the customer,the CRM name wins"""
    df = _df(spark, [
        _row(source="crm",         source_id="C1", email="a@x.com",
             first_name="Matthew", record_updated_at="2020-01-01"),
        _row(source="transaction", source_id="T1", email="a@x.com",
             first_name="Matt",    record_updated_at="2024-01-01"),
    ])
    resolved = processing.generate_identity(df)
    out = processing.reconcile(resolved).collect()
    assert out[0].first_name == "Matthew"