# Basic cleaning script to test pandas functions

import pandas as pd

df = pd.read_csv('out.csv')

# Drop first column
df = df.drop(df.columns[0], axis=1)

print(df.columns)