import sys
import pandas as pd
# python pipeline.py 12 123 1234
# sys.argv = [pipeline.py, 12, 123, 1234]
print('arguments', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

print(f'hello pipeline, month = {month}')