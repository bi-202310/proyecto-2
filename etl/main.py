
# From the CSV files on data folder, lets load it on a dataframe and do some ASUM-DM exploratory analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import seaborn as sns
import json
import psycopg2


# Load the data
df1 = pd.read_csv(os.path.join(
    'data', 'Datos_proyecto_II_BI_2017.csv'), encoding='latin1')
df2 = pd.read_csv(os.path.join(
    'data', 'Datos_proyecto_II_BI_2021.csv'), encoding='latin1')
df1 = df1.iloc[:, 1:]
df2 = df2.iloc[:, 1:]
df = pd.concat([df1, df2], ignore_index=True)

# Turn CODLOCALIDAD into a string
df["CODLOCALIDAD"] = df["CODLOCALIDAD"].astype(str)


with open(os.path.join('data', 'poblacion-upz-bogota.json')) as f:
    localidades = json.load(f)

# from localidades sabe the cod_loc, and the geo_point_2d in a dict
localidades = {loc["cod_loc"]: (
    loc["geo_point_2d"]["lon"], loc["geo_point_2d"]["lat"], loc["nomb_loc"]) for loc in localidades}

print(localidades)

# coords = {str(key): (coord["lon"], coord["lat"])
#           for key, coord in localidades.items()}

lons = {str(key): str(coord[0]) for key, coord in localidades.items()}
lats = {str(key): str(coord[1]) for key, coord in localidades.items()}
name = {str(key): str(coord[2]) for key, coord in localidades.items()}


# Create a new column with the lon of the localidad
df["lon"] = df["CODLOCALIDAD"].map(lons)

# Create a new column with the lat of the localidad
df["lat"] = df["CODLOCALIDAD"].map(lats)

# Create a new column with the name of the localidad
df["nombre_loc"] = df["CODLOCALIDAD"].map(name)

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
# - NVCBP14K: La vivienda está cerca de talleres de mecánica, servitecas o
# estaciones de gasolina?
# - NVCBP15D: El sector presenta contaminación del aire?
# - NPCFP1: Está afiliado a alguna entidad de seguridad social en salud?
# - NPCFP2: A cuál de los siguientes regímenes de seguridad social en salud
# está afiliado?
# - NPCFP14I: Está diagnosticado con asma?
# - NVCAP99: Fecha Apertura
# - CODLOCALIDAD: Código de la localidad


cols = ['NPCFP14I', 'NPCFP1', 'NVCBP8A', 'NVCBP8B', 'NVCBP8D', 'NVCBP8H', 'NVCBP14A', 'NVCBP14D',
        'NVCBP14K', 'NVCBP15D', 'NVCBP8G', 'NVCBP14B', 'NVCBP14H', 'CODLOCALIDAD', 'COD_UPZ']


# Only leave the 1 and 2 values, replace the rest with nan
for col in cols[:-2]:
    df[col] = df[col].replace(9, np.nan)


# If a value is nan replace it for postrgesql null
for col in cols:
    df[col] = df[col].replace(np.nan, None)


# Turn the 1s into False and 2s into True
for col in cols[:-2]:
    df[col] = df[col].replace(1, True)
    df[col] = df[col].replace(2, False)


# Open a conncetion to local postgres database with pyscopg2
conn = psycopg2.connect("dbname=asmacaso user=postgres")
cur = conn.cursor()

# Create a table called AsmaFact
cur.execute("DROP TABLE IF EXISTS AsmaFact CASCADE;")
cur.execute("""
CREATE TABLE AsmaFact (
	id integer GENERATED BY DEFAULT AS IDENTITY,
	asma boolean,
    afiliado boolean,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
""")
conn.commit()

# Create a table called HumedadDim
cur.execute("DROP TABLE IF EXISTS HumedadDim CASCADE;")
cur.execute("""
CREATE TABLE HumedadDim (
	id integer,
	humedadTechoParedes boolean,
	goteras boolean,
	fallaTuberias boolean,
	inundacion boolean,
    CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES AsmaFact(id) ON DELETE CASCADE
);
""")
conn.commit()

# Create a table called ContaminacionDim
cur.execute("DROP TABLE IF EXISTS ContaminacionDim CASCADE;")
cur.execute("""
CREATE TABLE ContaminacionDim (
	id integer,
	cercaFabricasIndustria boolean,
    cercaTerminalesBuses boolean,
    cercaTalleresMecanica boolean,
    contaminacionAire boolean,
    CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES AsmaFact(id) ON DELETE CASCADE
);
""")
conn.commit()

# Create a table called SalubridadDim
cur.execute("DROP TABLE IF EXISTS SalubridadDim CASCADE;")
cur.execute("""
CREATE TABLE SalubridadDim (
	id integer,
	escasaVentilacion boolean,
    cercaBotaderos boolean,
    cercaCentralesElectricas boolean,
    CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES AsmaFact(id) ON DELETE CASCADE
);
""")
conn.commit()

# Create a table called UbicacionDim
cur.execute("DROP TABLE IF EXISTS UbicacionDim CASCADE;")
cur.execute("""
CREATE TABLE UbicacionDim (
	id integer,
    UPZ text,
    localidad text,
    nombre_loc text,
    lon text,
    lat text,
    CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES AsmaFact(id) ON DELETE CASCADE
);
""")
conn.commit()


# Load the data from the dataframe to the database
for index, row in df.iterrows():
    # Insert into AsmaFact
    cur.execute("INSERT INTO AsmaFact (id, asma, afiliado) VALUES (%s, %s, %s);",
                (index, row['NPCFP14I'], row['NPCFP1']))
    conn.commit()
    # Insert into HumedadDim
    cur.execute("INSERT INTO HumedadDim (id, humedadTechoParedes, goteras, fallaTuberias, inundacion) VALUES (%s, %s, %s, %s, %s);",
                (index, row['NVCBP8A'], row['NVCBP8B'], row['NVCBP8D'], row['NVCBP8H']))
    # Insert into ContaminacionDim
    cur.execute("INSERT INTO ContaminacionDim (id, cercaFabricasIndustria, cercaTerminalesBuses, cercaTalleresMecanica, contaminacionAire) VALUES (%s, %s, %s, %s, %s);",
                (index, row['NVCBP14A'], row['NVCBP14D'], row['NVCBP14K'], row['NVCBP15D']))
    # Insert into SalubridadDim
    cur.execute("INSERT INTO SalubridadDim (id, escasaVentilacion, cercaBotaderos, cercaCentralesElectricas) VALUES (%s, %s, %s, %s);",
                (index, row['NVCBP8G'], row['NVCBP14B'], row['NVCBP14H']))
    # Insert into UbicacionDim
    cur.execute("INSERT INTO UbicacionDim (id, UPZ, localidad, nombre_loc, lon, lat) VALUES (%s, %s, %s, %s, %s, %s);",
                (index, row['COD_UPZ'], row['CODLOCALIDAD'], row['nombre_loc'], row['lon'], row['lat']))

conn.commit()

# Close the connection
cur.close()
conn.close()
