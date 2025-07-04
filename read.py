import pandas as pd

df = pd.read_parquet("output.parquet")
print(df.head(10))  # Prints the first 10 rows