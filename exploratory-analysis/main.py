
# From the CSV files on data folder, lets load it on a dataframe and do some ASUM-DM exploratory analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import seaborn as sns


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

# Take only these columns: 
# - NVCBP8A: La vivienda presenta humedades en el techo o en paredes?
# - NVCBP8B: La vivienda presenta goteras en el techo?
# - NVCBP8C: La vivienda presenta grietas en el techo y paredes?
# - NVCBP8D: La vivienda presenta fallas en las tuberías, cañerías o
# desagües?
# - NVCBP8E: La vivienda presenta grietas en el piso?
# - NVCBP8F: La vivienda presenta cielorrasos o tejas en mal estado?
# - NVCBP8G: La vivienda presenta escasa ventilación?
# - NVCBP8H: La vivienda presenta inundación cuando llueve o crece el rio?
# - NVCBP8I: La vivienda presenta peligro de derrumbe, avalancha o
# deslizamiento?
# - NVCBP8J: La vivienda presenta hundimiento del terreno?
# - NVCBP9: Algún espacio donde está ubicada la vivienda está dedicado a
# negocios de industria, comercio o servicios?
# - NVCBP14A: La vivienda está cerca de fábricas o industrias?
# - NVCBP14B: La vivienda está cerca de basureros o botaderos de basura?
# - NVCBP14D: La vivienda está cerca de terminales de buses?
# - NVCBP14H: La vivienda está cerca de líneas de alta tensión o centrales
# eléctricas?
# - NVCPB14K: La vivienda está cerca de talleres de mecánica, servitecas o
# estaciones de gasolina?
# - NVCBP15D: El sector presenta contaminación del aire?
# - NPCFP1: Está afiliado a alguna entidad de seguridad social en salud?
# - NPCFP2: A cuál de los siguientes regímenes de seguridad social en salud
# está afiliado?
# - NPCFP14I: Está diagnosticado con asma?
# - NVCAP99: Fecha Apertura
# - CODLOCALIDAD: Código de la localidad

df = df[['NVCBP8A', 'NVCBP8B', 'NVCBP8C', 'NVCBP8D', 'NVCBP8E', 'NVCBP8F', 'NVCBP8G', 'NVCBP8H', 'NVCBP8I', 'NVCBP8J', 'NVCBP9', 'NVCBP14A', 'NVCBP14B', 'NVCBP14D', 'NVCBP14H', 'NVCBP15D', 'NPCFP1', 'NPCFP2', 'NPCFP14I', 'NVCAP99', 'CODLOCALIDAD']]

print(df)

# show correlaation matrix
corr = df.corr()
sns.heatmap(corr, annot=True, cmap=plt.cm.Reds)
plt.show()
