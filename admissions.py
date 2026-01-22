import pandas as pd
from pathlib import Path
import logging

RAW_PATH = "C:/Users/goyam/AppData/Local/Temp/4f713ad6-2f4e-4e99-8152-b9399c067894_admissions.csv.gz.admissions.csv.gz/admissions.csv"
OUTPUT_PATH = "admissions.parquet"

# This file will be used to test the ingestion and Schema-First design of the admissions.csv.gz data


# Create a schema for the ingested data
ADMISSIONS_SCHEMA = {
    "subject_id": "Int64",
    "hadm_id": "Int64",
    "admittime": "datetime64[ns]",
    "dischtime": "datetime64[ns]",
    "deathtime": "datetime64[ns]",
    "admission_type": "string",
    "admit_provider_id": "string",
    "admission_location": "string",
    "discharge_location": "string",
    "insurance": "string",
    "language": "string",
    "marital_status": "string",
    "race": "string",
    "edregtime": "datetime64[ns]",
    "edouttime": "datetime64[ns]",
    "hospital_expire_flag": "Int64"
}

# Quick raw data extraction
def extract(path: Path) -> pd.DataFrame:
  return pd.read_csv(path)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Keep only expected columns (schema contract)
    df = df[list(ADMISSIONS_SCHEMA.keys())].copy()

    # 2. Enforce integer columns
    int_cols = [
        "subject_id",
        "hadm_id",
        "hospital_expire_flag"
    ]

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 3. Enforce datetime columns
    datetime_cols = [
        "admittime",
        "dischtime",
        "deathtime",
        "edregtime",
        "edouttime"
    ]

    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # 4. Enforce string columns
    string_cols = [
        "admission_type",
        "admit_provider_id",
        "admission_location",
        "discharge_location",
        "insurance",
        "language",
        "marital_status",
        "race"
    ]

    for col in string_cols:
        df[col] = df[col].astype("string")

    return df


def load(df: pd.DataFrame, output_path: Path):
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            output_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
    except Exception as e:
        raise IOError(f"Failed to write parquet to {output_path}: {e}")

# Validation function
def validate(df: pd.DataFrame):
    if df["subject_id"].isna().any():
        logging.warning("Null subject_id values detected")



def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Starting admissions ETL pipeline")

    try:
        df_raw = extract(RAW_PATH)
        logging.info(f"Extracted {len(df_raw)} raw rows")

        df_curated = transform(df_raw)
        logging.info(f"Transformed dataset to {len(df_curated)} curated rows")

        validate(df_curated)
        logging.info("Validation completed")

        load(df_curated, Path(OUTPUT_PATH))
        logging.info(f"Parquet written to {OUTPUT_PATH}")

    except Exception as e:
        logging.exception("Admissions ETL pipeline failed")
        raise

    logging.info("Admissions ETL pipeline completed successfully")

if __name__ == "__main__":
    main()
