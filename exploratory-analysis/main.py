
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
localidades = {loc["cod_loc"]: loc["geo_point_2d"] for loc in localidades}
coords = {str(key): (coord["lon"], coord["lat"])
          for key, coord in localidades.items()}

lons = {str(key): coord["lon"] for key, coord in localidades.items()}
lats = {str(key): coord["lat"] for key, coord in localidades.items()}

# Create a new column with the lon of the localidad
df["lon"] = df["CODLOCALIDAD"].map(lons)

# Create a new column with the lat of the localidad
df["lat"] = df["CODLOCALIDAD"].map(lats)

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

all_cols = ['DIRECTORIO', 'DIRECTORIO_HOG', 'DIRECTORIO_PER', 'SECUENCIA', 'SECUENCIA_P', 'ORDEN', 'NVCAP99', 'DPTOMPIO', 'CLASE', 'TOT_HOG_COMP', 'FEX_C', 'LOCALIDAD_TEX', 'COD_UPZ', 'CODLOCALIDAD', 'SECUENCIA.B', 'SECUENCIA_P.B', 'ORDEN.B', 'NVCBP1', 'NVCBP2', 'NVCBP3', 'NVCBP4', 'NVCBP5', 'NVCBP6', 'NVCBP7', 'NVCBP8A', 'NVCBP8B', 'NVCBP8C', 'NVCBP8D', 'NVCBP8E', 'NVCBP8F', 'NVCBP8G', 'NVCBP8H', 'NVCBP8I', 'NVCBP8J', 'NVCBP9', 'NVCBP10', 'NVCBP11A', 'NVCBP11AA', 'NVCBP11B', 'NVCBP11C', 'NVCBP11D', 'NVCBP11DA', 'NVCBP12', 'NVCBP13', 'NVCBP14A', 'NVCBP14B', 'NVCBP14C', 'NVCBP14D', 'NVCBP14E', 'NVCBP14F', 'NVCBP14G', 'NVCBP14H', 'NVCBP14I', 'NVCBP14J', 'NVCBP14K', 'NVCBP15A', 'NVCBP15B', 'NVCBP15C', 'NVCBP15D', 'NVCBP15E', 'NVCBP15F', 'NVCBP15G', 'NVCBP15H', 'SECUENCIA_P.C', 'ORDEN.C', 'NHCCP1', 'NHCCP2', 'NHCCP3', 'NHCCP4A', 'NHCCP4B', 'NHCCP4C', 'NHCCP4D', 'NHCCP4E', 'NHCCP4F', 'NHCCP5', 'NHCCP6', 'NHCCP7', 'NHCCP8A', 'NHCCP8B', 'NHCCP8C', 'NHCCP8D', 'NHCCP8E', 'NHCCP8F', 'NHCCP8G', 'NHCCP8H', 'NHCCP8I', 'NHCCP8J', 'NHCCP9', 'NHCCP10', 'NHCCP11', 'NHCCP12', 'NHCCP13A', 'NHCCP13B', 'NHCCP13C', 'NHCCP13D', 'NHCCP13E', 'NHCCP13F', 'NHCCP13G', 'NHCCP13H', 'NHCCP13I', 'NHCCP13J', 'NHCCP14', 'NHCCP14A', 'NHCCP15', 'NHCCP15A', 'NHCCPCTRL2', 'NHCCP19', 'NHCCP20', 'NHCCP20A', 'NHCCP21A', 'NHCCP21B', 'NHCCP22A', 'NHCCP22B', 'NHCCP22C', 'NHCCP22D', 'NHCCP22E', 'NHCCP22F', 'NHCCP22G', 'NHCCP23', 'NHCCP24', 'NHCCP25', 'NHCCP26', 'NHCCP26A', 'NHCCP27', 'NHCCP28', 'NHCCP28A', 'NHCCP29', 'NHCCP29A', 'NHCCP31', 'NHCCP32', 'NHCCP33', 'NHCCP34', 'NHCCP35', 'NHCCP35A', 'NHCCP36A', 'NHCCP36B', 'NHCCP36C', 'NHCCP36D', 'NHCCP37', 'NHCCP38', 'NHCCP38A', 'NHCCP38B', 'NHCCP38C', 'NHCCP38D', 'NHCCP38E', 'NHCCP38F', 'NHCCP38G', 'NHCCP39A', 'NHCCP39B', 'NHCCP393', 'NHCCP39C', 'NHCCP39D', 'NHCCP39E', 'NHCCP39F', 'NHCCP39G', 'NHCCP399', 'NHCCP40A', 'NHCCP40B', 'NHCCP40C', 'NHCCP40D', 'NHCCP40E', 'NHCCP40F', 'NHCCP40G', 'NHCCP40H', 'NHCCP40I', 'NHCCP40J', 'NHCCP40K', 'NHCCP40L', 'NHCCP40M', 'NHCCP40N', 'NHCCP40O', 'NHCCP40P', 'NHCCP41', 'NHCCP41A', 'NHCCP41B', 'NHCCP42', 'NHCCP42A', 'NHCCP42CB', 'NHCCP44', 'NHCCP44A', 'NHCCP44B', 'NHCCP45', 'NHCCP45A', 'NHCCP45B', 'NHCCP46A', 'NHCCP46B', 'NHCCP46C', 'NHCCP46D', 'NHCCP46E', 'NHCCP46F', 'NHCCP46G', 'NHCCP46H', 'NHCCP46I', 'NHCCP46J', 'NHCCP46K', 'NHCCP46L', 'NHCCP46M', 'SECUENCIA_P.D', 'ORDEN.D', 'NHCDP1', 'NHCDP2', 'NHCDP2A', 'NHCDP2B', 'NHCDP3', 'NHCDP4', 'NHCDP4A', 'NHCDP4B', 'NHCDP5', 'NHCDP6', 'NHCDP6A', 'NHCDP6B', 'NHCDP7', 'NHCDP7A', 'NHCDP8', 'NHCDP9', 'NHCDP10', 'NHCDP10A', 'NHCDP11', 'NHCDP11A', 'NHCDP11B', 'NHCDP12', 'NHCDP13', 'NHCDP14A', 'NHCDP14B', 'NHCDP14C', 'NHCDP14D', 'NHCDP15', 'NHCDP16', 'NHCDP17', 'NHCDP17A', 'NHCDP18', 'NHCDP18A', 'NHCDP18B', 'NHCDP19', 'NHCDP20', 'NHCDP21', 'NHCDP21A', 'NHCDP22', 'NHCDP23', 'NHCDP23A', 'NHCDP23B', 'NHCDP24', 'NHCDP24A', 'NHCDP25', 'NHCDP25A', 'NHCDP26', 'NHCDP26A', 'NHCDP27', 'NHCDP27A', 'NHCDP27B', 'NHCDP28', 'NHCDP29', 'NHCDP29A', 'NHCDP29B', 'NHCDP30_1', 'NHCDP30A', 'NHCDP30B', 'NHCDP30_2', 'NHCDP30_3', 'NHCDP30_4', 'NHCDP31', 'NHCDP32', 'NHCDP32A', 'NHCDP32B', 'SECUENCIA_P.E', 'ORDEN.E', 'NPCEP4', 'NPCEP5', 'NPCEP6', 'NPCEP7', 'NPCEP8', 'NPCEP8A', 'NPCEP9', 'NPCEP9A', 'NPCEP9B', 'NPCEP10', 'NPCEP11A', 'NPCEP11AA', 'NPCEP11AB', 'NPCEP11AC', 'NPCEP11', 'NPCEP13', 'NPCEP13A', 'NPCEP13B', 'NPCEP13C', 'NPCEP14', 'NPCEP15', 'NPCEP16A', 'NPCEP16B', 'NPCEP16C', 'NPCEP16D', 'NPCEP16E', 'NPCEP16F', 'NPCEP16G', 'NPCEP16H', 'NPCEP16I', 'NPCEP16J', 'NPCEP16K', 'NPCEP16A1', 'NPCEP16AA', 'NPCEP16AB', 'NPCEP16B1', 'NPCEP17', 'NPCEP18', 'NPCEP19', 'NPCEP21', 'NPCEP21A', 'NPCEP22', 'NPCEP22A', 'NPCEP24', 'NPCEP24A', 'NPCEP25', 'NPCEP25A', 'NPCEP27', 'NPCEP26', 'NPCEP5A', 'SECUENCIA_P.F', 'ORDEN.F', 'NPCFP1', 'NPCFP2', 'NPCFP3', 'NPCFP4A', 'NPCFP4B', 'NPCFP4C', 'NPCFP4D', 'NPCFP4E', 'NPCFP5', 'NPCFP7', 'NPCFP8', 'NPCFP8A', 'NPCFP8B', 'NPCFP9', 'NPCFP10A', 'NPCFP10B', 'NPCFP10C', 'NPCFP10D', 'NPCFP10E', 'NPCFP11', 'NPCFP11A', 'NPCFP12', 'NPCFP13A', 'NPCFP13B', 'NPCFP13C', 'NPCFP13D', 'NPCFP13E', 'NPCFP14A', 'NPCFP14B', 'NPCFP14C', 'NPCFP14D', 'NPCFP14E', 'NPCFP14F', 'NPCFP14G', 'NPCFP14H', 'NPCFP14I', 'NPCFP14J', 'NPCFP14K', 'NPCFP14L', 'NPCFP15', 'NPCFP16', 'NPCFP18', 'NPCFP19', 'NPCFP20', 'NPCFP21A',
            'NPCFP21B', 'NPCFP21C', 'NPCFP21D', 'NPCFP21E', 'NPCFP21F', 'NPCFP21G', 'NPCFP21H', 'NPCFP21I', 'NPCFP22', 'NPCFP23', 'NPCFP24', 'NPCFP24A', 'NPCFP24B', 'NPCFP24C', 'NPCFP25A', 'NPCFP25B', 'NPCFP25BA', 'NPCFP25C', 'NPCFP25E', 'NPCFP25F', 'NPCFP26', 'NPCFP26A', 'NPCFP27', 'NPCFP29', 'NPCFP30', 'NPCFP31', 'NPCFP32', 'NPCFP33A', 'NPCFP33AA', 'NPCFP33B', 'NPCFP33BA', 'NPCFP33C', 'NPCFP33CA', 'NPCFP33D', 'NPCFP33DA', 'NPCFP33E', 'NPCFP33EA', 'NPCFP33F', 'NPCFP33FA', 'NPCFP34', 'NPCFP35A', 'NPCFP35AA', 'NPCFP35B', 'NPCFP35BA', 'NPCFP36', 'NPCFP37', 'NPCFP38', 'NPCFP39', 'NPCFP39A', 'NPCFP40A', 'NPCFP40B', 'NPCFP40C', 'NPCFP40D', 'NPCFP40E', 'NPCFP40F', 'NPCFP40', 'NPCFP41', 'NPCFP42', 'NPCFP43', 'NPCFP43A', 'NPCFP44', 'NPCFP45', 'SECUENCIA_P.G', 'ORDEN.G', 'SECUENCIA_P.H', 'ORDEN.H', 'NPCHP1', 'NPCHP2', 'NPCHP3', 'NPCHP4', 'NPCHP4A', 'NPCHP5', 'NPCHP6', 'NPCHP6A', 'NPCHP7', 'NPCHP9A', 'NPCHP9B', 'NPCHP9C', 'NPCHP9D', 'NPCHP9E', 'NPCHP9F', 'NPCHP10', 'NPCHP10A', 'NPCHP11', 'NPCHP11A', 'NPCHP12', 'NPCHP12A', 'NPCHP13', 'NPCHP13A', 'NPCHP13B', 'NPCHP14', 'NPCHP15A', 'NPCHP15B', 'NPCHP16', 'NPCHP17', 'NPCHP18A', 'NPCHP18B', 'NPCHP18C', 'NPCHP18D', 'NPCHP18E', 'NPCHP18F', 'NPCHP18G', 'NPCHP18H', 'NPCHP18I', 'NPCHP18J', 'NPCHP18K', 'NPCHP18L', 'NPCHP18M', 'NPCHP19B', 'NPCHP20', 'NPCHP20A', 'NPCHP20B', 'NPCHP21A', 'NPCHP21AA', 'NPCHP21B', 'NPCHP21BA', 'NPCHP21C', 'NPCHP21CA', 'NPCHP22', 'NPCHP22A', 'NPCHP23', 'NPCHP23A', 'NPCHP24', 'NPCHP24_1A', 'NPCHP24AA', 'NPCHP24AB', 'NPCHP24_1B', 'NPCHP24BA', 'NPCHP24BB', 'NPCHP25A', 'NPCHP25B', 'NPCHP25C', 'NPCHP25D', 'NPCHP25E', 'NPCHP25F', 'NPCHP25G', 'NPCHP25H', 'NPCHP25I', 'NPCHP28', 'NPCHP28A', 'NPCHP28B', 'NPCHP29A', 'NPCHP29B', 'NPCHP29C', 'NPCHP29D', 'NPCHP29E', 'NPCHP29F', 'NPCHP29G', 'NPCHP29H', 'NPCHP29I', 'NPCHP30A', 'NPCHP30B', 'NPCHP30C', 'NPCHP30D', 'NPCHP30E', 'NPCHP30F', 'NPCHP30G', 'NPCHP30H', 'NPCHP30I', 'NPCHP30J', 'NPCHP30K', 'NPCHP30L', 'NPCHP31AA', 'NPCHP31AB', 'NPCHP31BA', 'NPCHP31BB', 'NPCHP31CA', 'NPCHP31CB', 'NPCHP31DA', 'NPCHP31DB', 'NPCHP31EA', 'NPCHP31EB', 'NPCHP31FA', 'NPCHP31FB', 'NPCHP32', 'NPCHP32A', 'NPCHP32B', 'NPCHP33', 'NPCHP34', 'NPCHP34A', 'NPCHP35A', 'NPCHP35B', 'NPCHP35C', 'NPCHP35D', 'NPCHP35E', 'NPCHP35F', 'NPCHP35J', 'NPCHP35I', 'DPTO', 'MPIO', 'COD_LOCALIDAD', 'NOMBRE_LOCALIDAD', 'COD_UPZ_GRUPO', 'NOMBRE_UPZ_GRUPO', 'ESTRATO2021', 'NOMBRE_ESTRATO', 'NVCBP9A1', 'NVCBP9A2', 'NVCBP9A3', 'NVCBP9A4', 'NVCBP14L', 'NVCBP15I', 'NVCBP15J', 'NVCBP15K', 'NVCBP15L', 'NVCBP15M', 'NVCBP16A1', 'NVCBP16A2', 'NVCBP16A3', 'NVCBP16A4', 'NVCBP16', 'NHCCP8_1', 'NHCCP8_2', 'NHCCP8_3', 'NHCCP8_4', 'NHCCP8_5', 'NHCCP8_6', 'NHCCP8_7', 'NHCCP8_8', 'NHCCP8_9', 'NHCCP8_10', 'NHCCP10A', 'NHCCP10B', 'NHCCP10C', 'NHCCP10D', 'NHCCP10E', 'NHCCP11A', 'NHCCP19A', 'NHCCP38AA', 'NHCCP38AB', 'NHCCP38AC', 'NHCCP38AD', 'NHCCP38AF', 'NHCCP38AG', 'NHCCP41_A', 'NHCCP41_B', 'NHCCP47A', 'NHCCP47A1', 'NHCCP47B', 'NHCCP47B1', 'NHCCP47C', 'NHCDP33', 'NHCDP33A', 'NPCEP_5', 'SEXO', 'NPCEP9C', 'NPCEP11D', 'NPCEP13D', 'NPCEP16_1', 'NPCEP16A_1', 'NPCEP16B_1', 'NPCEP16D_1', 'NPCEP17_1', 'NPCEP16_1A', 'NPCEP16_2A', 'NPCEP16_3A', 'NPCEP16_4A', 'NPCEP16_5A', 'NPCEP16_6A', 'NPCEP16_7A', 'NPCEP16_8A', 'NPCEP21A_1', 'NPCEP21A_2M', 'NPCEP21B_1', 'NPCEP21B_2', 'NPCEP21B_3', 'NPCEP21B_4', 'NPCEP21B_5', 'NPCEP21C_1', 'NPCEP29', 'NPCEP29A', 'NPCFP7A1', 'NPCFP7A2', 'NPCFP7A3', 'NPCFP7A4', 'NPCFP8A2', 'NPCFP8A3', 'NPCFP8A4', 'NPCFP8A5', 'NPCFP8A6', 'NPCFP8A7', 'NPCFP8A8', 'NPCFP13F', 'NPCFP21A1', 'NPCFP21A2', 'NPCFP21A3', 'NPCFP21A4', 'NPCFP21A5', 'NPCFP21A6', 'NPCFP21A7', 'NPCFP21A8', 'NPCFP22A1', 'NPCFP22A2', 'NPCFP22A21', 'NPCFP22A3', 'NPCFP22A4', 'NPCFP25D', 'NPCFP33G', 'NPCFP33GA', 'NPCFP33H', 'NPCFP33HA', 'NPCFP40G', 'NPCFP40A1', 'NPCFP40B2', 'NPCFP41A', 'NPCFP46', 'NPCFP47A', 'NPCFP47B', 'NPCFP47C', 'NPCFP47D', 'NPCFP47E', 'NPCFP47F', 'NPCHP18AA', 'NPCHP18AB', 'NPCHP18AC', 'NPCHP18AD', 'NPCHP18AE', 'NPCHP18AF', 'NPCHP18AG', 'NPCHP18AH', 'NPCHP18AI', 'NPCHP18AI1', 'NPCHP18AJ', 'NPCHP18AK', 'NPCHP18AL', 'NPCHP18AM', 'NPCHP18AN', 'NPCHP18AO', 'NPCHP19', 'NPCHP21D', 'NPCHP21DA', 'NPCHP24_1', 'NPCHP24_2', 'NPCHP25J', 'NPCHP31DA_1', 'NPCHP31DB_1', 'NPCHP31GA', 'NPCHP31GB', 'NPCHP31DA_2', 'NPCHP31DB_2', 'NPCHP35K', 'NPCHP36', 'NPCHP36A', 'NPCHP37']

cols = ['NPCFP14I', 'NPCFP1', 'NVCBP8A', 'NVCBP8B', 'NVCBP8D', 'NVCBP8H', 'NVCBP14A', 'NVCBP14D',
        'NVCBP14K', 'NVCBP15D', 'NVCBP8G', 'NVCBP14B', 'NVCBP14H', 'CODLOCALIDAD', 'COD_UPZ']

# print(df[cols].head(5))

# for col in cols:
#     print(sorted(df[col].unique()))

# Only leave the 1 and 2 values, replace the rest with nan
for col in cols[:-2]:
    df[col] = df[col].replace(9, np.nan)


# print()
# for col in cols:
#     print(sorted(df[col].unique()))

# If a value is nan replace it for postrgesql null
for col in cols:
    df[col] = df[col].replace(np.nan, None)

# print(df.shape)

# Turn the 1s into False and 2s into True
for col in cols[:-2]:
    df[col] = df[col].replace(1, True)
    df[col] = df[col].replace(2, False)

# print(df.shape)

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

# Create a table called UbiacionDim
cur.execute("DROP TABLE IF EXISTS UbiacionDim CASCADE;")
cur.execute("""
CREATE TABLE UbiacionDim (
	id integer,
    UPZ text,
    localidad text,
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
    # Insert into UbiacionDim
    cur.execute("INSERT INTO UbiacionDim (id, UPZ, localidad, lon, lat) VALUES (%s, %s, %s, %s, %s);",
                (index, row['COD_UPZ'], row['CODLOCALIDAD'], row['lon'], row['lat']))

conn.commit()

# Close the connection
cur.close()
conn.close()
