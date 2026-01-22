import pandas as pd

# Very quick way to make sure that the admissions.parquet output works.

df = pd.read_parquet("admissions.parquet")
print(df.head())
print(df.info())