
# From the CSV files on data folder, lets load it on a dataframe and do some ASUM-DM exploratory analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Load the data
df1 = pd.read_csv(os.path.join('data', 'Datos_proyecto_II_BI_2017.csv'), encoding= 'latin1')
df2 = pd.read_csv(os.path.join('data', 'Datos_proyecto_II_BI_2021.csv'), encoding= 'latin1')

#Join the dataframes as one on the same columns
df = pd.concat([df1, df2], ignore_index=True)


# Lets see the data
print(df.head())
print("--------------------")

# Lets see the data types
print(df.dtypes)
print("--------------------")

# Lets see the data shape
print(df.shape)
print("--------------------")

# Lets see the data columns
print(df.columns)
print("--------------------")

# Lets see the data info
print(df.info())
print("--------------------")

# Lets see the data describe
print(df.describe())
print("--------------------")

# Lets see the data null values
print(df.isnull().sum())
