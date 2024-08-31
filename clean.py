# Basic cleaning script to test pandas functions

import pandas as pd

df = pd.read_csv('out.csv')

# Drop first column
df = df.drop(df.columns[0], axis=1)

# Drop rows where critical fields have missing values
critical_fields = ['Date', 'Brand Name(s)', 'Product Description', 'Product Type']
df = df.dropna(subset=critical_fields)

# Replace missing values in non-critical fields with "Not provided"
df.fillna(value="Not provided", inplace=True)

# Convert the Date column to pandas datetime.date format (Only has year, month, day)
df['Date'] = pd.to_datetime(df['Date']).dt.date

# Ensure all other columns are of type string
for col in df.columns:
    if col != 'Date':
        df[col] = df[col].astype(str)

print(df)
print()
print("Missing values:")
print(df.isna().sum())
print()
print(df)