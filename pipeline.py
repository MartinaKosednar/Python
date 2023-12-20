import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

df = pd.read_csv("housing_in_london_monthly_variables.csv")

# Converting the 'date' column to datetime format
df['date'] = pd.to_datetime(df['date'])
print(df.info())

# Counting and displaying the number of missing values in each column
missing_values = df.isna().sum()
print(missing_values)

# Droping the no_of_crimes column since have NaN values 
df = df.drop(['no_of_crimes'], axis='columns')

# Fill in missing values in 'houses_sold' with the mean grouped by 'area'
df['houses_sold'].fillna(df.groupby('area')['houses_sold'].transform('mean'), inplace=True)

# Extracting the year from the 'date' column and add it as a new 'year' column
df['year'] = df['date'].dt.year

print(df.head(10))

#Loading the data into showflake 

conn = snowflake.connector.connect(
    user='MARTINAK',
    password='**********',
    account='XPCISJB-QV52752',
    warehouse='DATA',
    database='DATA',
    schema='DATA_SCHEMA',
    role='ACCOUNTADMIN'
)

df.reset_index(drop = True, inplace = True)
success, nchunks, nrows, _ = write_pandas(conn, df, "HOUSING", auto_create_table = True)
print(str(success)+ "," +str(nchunks) + "," + str(nrows))
conn.close()