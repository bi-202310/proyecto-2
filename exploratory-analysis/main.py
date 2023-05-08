
# From the CSV files on data folder, lets load it on a dataframe and do some ASUM-DM exploratory analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Load the data
df1 = pd.read_csv(os.path.join('data', 'Datos_proyecto_II_BI_2017.csv'), encoding= 'latin1')
#print(len(df1.columns))
#print(len(df1.index))
df2 = pd.read_csv(os.path.join('data', 'Datos_proyecto_II_BI_2021.csv'), encoding= 'latin1')
#print(len(df2.columns))
#print(len(df2.index))
df1 = df1.iloc[:, 1:]
df2 = df2.iloc[:, 1:]

#print(df1)

#ls = list(filter(lambda x: x not in df1.columns or x not in df2.columns, set(list(df1.columns) + list(df2.columns))))
#print(len(ls))

# 445 in both
# 263 that are either in one or the other
# 708 total
# Combine the dataframes into one where the columns are the same
df = pd.concat([df1, df2], axis=0, ignore_index=True)

# # Lets see the data
# print(df.head())
# print("--------------------")

# # Lets see the data types
# print(df.dtypes)
# print("--------------------")

# # Lets see the data shape
# print(df.shape)
# print("--------------------")

# # Lets see the data columns
# print(df.columns)
# print("--------------------")

# # Lets see the data info
# print(df.info())
# print("--------------------")

# # Lets see the data describe
# print(df.describe())
# print("--------------------")

# # Lets see the data null values
# print(df.isnull().sum())
