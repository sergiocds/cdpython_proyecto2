# %% [markdown]
# # Proyecto 2
# # Ingeniería de Datos con Python
# # Universidad Galileo
# # Instituto de Investigación de Operaciones
# ## Master en Data Science
# # -------------------------------------------------------------
# 
# ### Integrantes del grupo
# ### Castillo Custodio, Sergio Josué - carnet 23000331
# ### Peña Maltez, Andres Alberto - carnet 23004061

# %% [markdown]
# #### Importando Librerias

# %%
import pandas as pd
import numpy as np
from faker import Faker
import random
import datetime
import boto3
import psycopg2
import configparser
import pymysql
from datetime import datetime, timedelta


# %% [markdown]
# # Creando Modelo Relacional y DDL

# %% [markdown]
# #### Cargamos archivo de configuraciones

# %%
config = configparser.ConfigParser()
config.read('escec.cfg')


# %% [markdown]
# #### Creamos Instancia de RDS 

# %%
aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')

# %% [markdown]
# #### Verificamos Instancias de RDS disponibles

# %%
rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")

# %% [markdown]
# #### Creación de Servicio RDS

# %%
rdsIdentifier = 'viajes'

# %%
try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS_MYSQL', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername=config.get('RDS_MYSQL', 'DB_USER'),
            MasterUserPassword=config.get('RDS_MYSQL', 'DB_PASSWORD'),
            Port=int(config.get('RDS_MYSQL', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")

# %% [markdown]
# ##### Recordemos Esperar unos minutos para consultar la informaicón de la instancia.

# %% [markdown]
# ##### Obtenemos URL del Host

# %%
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

# %% [markdown]
# ##### Conexión a Base de Datos desde Python

# %%
import create_travels_DB
import pymysql
from pymysql.constants import CLIENT

try:
    myDDL = pymysql.connect(host=RDS_HOST, user=config.get('RDS_MYSQL', 'DB_USER'), password=config.get('RDS_MYSQL', 'DB_PASSWORD'), client_flag = CLIENT.MULTI_STATEMENTS)

    mycursor = myDDL.cursor()

        #Lets's create a DB
    sql = '''create database if not exists viajes'''
    mycursor.execute(sql)
    mycursor.connection.commit()
    
    sql = '''use viajes'''
    mycursor.execute(sql)
    mycursor.connection.commit()
    mycursor = myDDL.cursor()
    mycursor.execute(create_travels_DB.CREATE_DB)
    mycursor.connection.commit()

    mycursor.close()
    
    

    
    print("Base de Datos Creada Exitosamente")
except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
	print("Ocurrió un error al conectar: ", e)
    
myDDL


# %% [markdown]
# #### Estableciendo la Conexión desde S3 Buckets

# %%
s3 = boto3.resource(
	service_name = 's3',
	region_name ='us-east-1',
	aws_access_key_id = config.get('IAM','ACCESS_KEY'),
	aws_secret_access_key = config.get('IAM','SECRET_ACCESS_KEY')
)

# %% [markdown]
# #### Cargando data desde CSV hacia el dataframe

# %%
# Establecer el nombre del bucket y los nombres de archivo que quieres descargar
bucket_name = 'travels2023'
file_names = ['flights.csv', 'hotels.csv', 'users.csv']

# Descargar los archivos y guardarlos en un data frame separado
users_csv = pd.read_csv(s3.Object(bucket_name, file_names[0]).get()['Body'], delimiter=';')
hotels_csv = pd.read_csv(s3.Object(bucket_name, file_names[1]).get()['Body'], delimiter=';')
flights_csv = pd.read_csv(s3.Object(bucket_name, file_names[2]).get()['Body'], delimiter=';')

# %% [markdown]
# #### Users

# %%
# Renombrando columnas del DataFrame para que coincidan con atributos de viajes
users_csv.rename(columns={'code':'id','name':'name_'},inplace=True)

# %% [markdown]
# #### Hotels

# %%
# Agrupando datos de hoteles por nombre y lugar del hotel para obtener registros únicos
hotels_clean=pd.DataFrame(hotels_csv.groupby(['name','place']).count())
# Eliminando campos que no se necesitarán
hotels_clean.drop(columns=['travelCode','userCode','days','price','total','date'],inplace=True)
# Reiniciando índices
hotels_clean.reset_index(inplace=True)
# Renombrando columnas del DataFrame para que coincidan con atributos de travels_DB
hotels_clean.rename(columns={'name':'name_'},inplace=True)

# %% [markdown]
# ##### Travels

# %%
# Obteniendo viajes realizados
# Tabla de hoteles
travels_h=hotels_csv[['travelCode','userCode']]
# Tabla de viajes
travels_f=flights_csv[['travelCode','userCode']]
# Uniendo los dos DataFrames
travels=pd.concat([travels_f,travels_h],ignore_index=True)
# Eliminando duplicados
travels.drop_duplicates(subset=['travelCode'],inplace=True)
# Renombrando columnas del DataFrame para que coincidan con atributos de travels_DB
travels.rename(columns={'travelCode':'id','userCode':'user_id'},inplace=True)

# %% [markdown]
# #### Flights

# %%
# Eliminando columna userCode
flights=flights_csv.drop(['userCode'],axis=1)
# Renombrando columnas del DataFrame para que coincidan con atributos de travels_DB
flights.rename(columns={'travelCode':'travel_id','from':'origin','to':'destination','flightType':'flight_type','time':'time_','date':'departure_date'},inplace=True)
# Modificando formato de fecha para departure_date
flights['departure_date']=pd.to_datetime(flights['departure_date'],format='%m/%d/%Y')
flights['departure_date']=flights['departure_date'].values.astype('datetime64[us]')

# %% [markdown]
# ##### Creamos SQL_Driver

# %%
def insertDataToSQL(data_dict, table_name):
    mysql_driver = f"""mysql+pymysql://{config.get('RDS_MYSQL', 'DB_USER')}:{config.get('RDS_MYSQL', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS_MYSQL', 'DB_PORT')}/{config.get('RDS_MYSQL', 'DB_NAME')}"""  
    df_data = pd.DataFrame.from_records(data_dict)
    try:
          response = df_data.to_sql(table_name, mysql_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
    except Exception as ex:
          print(ex)


# %% [markdown]
# #### Cargando Datasets

# %% [markdown]
# #### Insertamos datos a tablas en RDS

# %%
# Insertando datos en tablas de DB Viajes
insertDataToSQL(users_csv, 'USERS')
insertDataToSQL(hotels_clean, 'HOTELS')
insertDataToSQL(travels, 'TRAVELS')
insertDataToSQL(flights, 'FLIGHTS')

# %% [markdown]
# #### Transformando y preparando Data para HotelStays

# %%
import pymysql
from pymysql.constants import CLIENT

# Recuperando información de hoteles actualizada

try:
    myDDL = pymysql.connect(host=RDS_HOST, user=config.get('RDS_MYSQL', 'DB_USER'), password=config.get('RDS_MYSQL', 'DB_PASSWORD'), client_flag = CLIENT.MULTI_STATEMENTS)
    mycursor = myDDL.cursor()

    sql = '''use viajes'''
    mycursor.execute(sql)
    mycursor.connection.commit()
    sql = 'SELECT * FROM HOTELS;'
    mycursor.execute(sql)
    hotels_u = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()
    mycursor.close()
  
    print("Base de Datos Creada Exitosamente")
except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
	print("Ocurrió un error al conectar: ", e)

# Renombrando columnas
hotels_u.rename(columns={0:'id',1:'name_',2:'place'},inplace=True)
# Completando id del hotel desde dataframe hotels_u
stays=hotels_csv.merge(hotels_u,how='left',left_on='name',right_on='name_')
# Eliminando columnas userCode, name, place_x, name_, place_y
stays=stays.drop(['userCode','name','place_x','name_','place_y'],axis=1)
# Modificando formato de fecha para date
stays['date']=pd.to_datetime(stays['date'],format='%m/%d/%Y')
stays['date']=stays['date'].values.astype('datetime64[us]')
# Renombrando columnas del DataFrame para que coincidan con atributos de travels_DB
stays.rename(columns={'travelCode':'travel_id','days':'days_','date':'check_in','id':'hotel_id'},inplace=True)
stays
    



print(stays)


# %% [markdown]
# #### Poblando tabla Hotel Stays

# %%
insertDataToSQL(stays, 'HOTELS_STAYS')

# %%
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

# %% [markdown]
# #  Creando Dimensiones y DW

# %%
import pymysql
from pymysql.constants import CLIENT

# Recuperando información de hoteles actualizada

try:
    myDDL = pymysql.connect(host='viajes.cnelrr1vhuaq.us-east-1.rds.amazonaws.com', user=config.get('RDS_MYSQL', 'DB_USER'), password=config.get('RDS_MYSQL', 'DB_PASSWORD'), client_flag = CLIENT.MULTI_STATEMENTS)
    mycursor = myDDL.cursor()


    sql = 'use viajes' 
    mycursor.execute(sql)
    mycursor.connection.commit()


    sql='SELECT * FROM FLIGHTS;'
    mycursor.execute(sql)
    column_Names  = mycursor.description    
    #flightsdb_df = [{columns[index][0]: column for index, column in enumerate(value)} for value in mycursor.fetchall()]
    flightsdb_df = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()     
    
        
    sql = 'SELECT * FROM HOTELS;'
    mycursor.execute(sql)
    hotels_u = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()
    #
    sql='SELECT * FROM USERS;'
    mycursor.execute(sql)
    usersdb_df = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()
    #
    sql='SELECT * FROM TRAVELS;'
    mycursor.execute(sql)
    travelsdb_df = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()
    #
    sql='SELECT * FROM HOTELS;'
    mycursor.execute(sql)
    hotelsdb_df = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit()
    #
    sql='SELECT * FROM HOTELS_STAYS;'
    mycursor.execute(sql)
    staysdb_df = pd.DataFrame(mycursor.fetchall())
    mycursor.connection.commit() 
    #

    
    mycursor.close()
  
    print("Base de Datos Creada Exitosamente")
except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
	print("Ocurrió un error al conectar: ", e)
    
    
    

# %% [markdown]
# ### Creando Dimensiones

# %% [markdown]
# #### Renombrando Columnas de DataSets

# %%
# Renombrando columnas del DataFrame 
hotels_u.rename(columns={0:'id',1:'name',2:'place'},inplace=True)
usersdb_df.rename(columns={0:'id',1:'name_',2:'company',3:'age',4:'gender'},inplace=True)
travelsdb_df.rename(columns={0:'id',1:'user_id'},inplace=True)
hotelsdb_df.rename(columns={0:'id',1:'hotel_id',2:'travel_id',3:'check_in',4:'days',5:'price',6:'total'},inplace=True)
staysdb_df.rename(columns={0:'id',1:'hotel_id',2:'travel_id',3:'check_in',4:'days_',5:'price',6:'total'},inplace=True)
flightsdb_df.rename(columns={0:'id',1:'travel_id',2:'origin',3:'destination',4:'flight_type',5:'agency',6:'departure_date',7:'price',8:'time_',9:'distance'},inplace=True)


# %% [markdown]
# #### Time_dim (Dimensión de tiempo)

# %%
# Obteniendo fechas mínimas y máximas de vuelos
flightsmin=flightsdb_df['departure_date'].min()
flightsmax=flightsdb_df['departure_date'].max()
print(f'Los vuelos considerados están comprendidos entre {flightsmin} y {flightsmax}.')
# Obteniendo fechas mínimas y máximas de reservaciones de hotel
hotelsmin=staysdb_df['check_in'].min()
hotelsmax=staysdb_df['check_in'].max()
print(f'Las reservaciones de hotel considerados están comprendidos entre {hotelsmin} y {hotelsmax}.')

# %%
# Generando fechas entre valores máximos y mínimos.
Time_dim_df=pd.DataFrame(pd.date_range(flightsmin,flightsmax,freq='D'))
Time_dim_df.columns=['Full_date']
# Generando Date_key
Time_dim_df['Date_key']=(pd.DatetimeIndex(Time_dim_df['Full_date']).year)*10000+(pd.DatetimeIndex(Time_dim_df['Full_date']).month)*100+pd.DatetimeIndex(Time_dim_df['Full_date']).day
# Generando Day_of_week. Se suma una unidad para que semana inicie en 1=Lunes.
Time_dim_df['Day_of_week']=pd.DatetimeIndex(Time_dim_df['Full_date']).dayofweek+1
# Generando Day_num_in_month
Time_dim_df['Day_num_in_month']=pd.DatetimeIndex(Time_dim_df['Full_date']).day
# Generando Day_num_overall
Time_dim_df['Day_num_overall']=Time_dim_df.index+1
# Generando Day_name
Time_dim_df['Day_name']=pd.DatetimeIndex(Time_dim_df['Full_date']).day_name()
# Generando Day_abbrev
Time_dim_df['Day_abbrev']=Time_dim_df['Day_name'].str.slice(0,3)
# Generando Weekday_flag
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Mon','Weekday_flag']='Weekday'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Tue','Weekday_flag']='Weekday'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Wed','Weekday_flag']='Weekday'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Thu','Weekday_flag']='Weekday'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Fri','Weekday_flag']='Weekday'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Sat','Weekday_flag']='Weekend'
Time_dim_df.loc[Time_dim_df['Day_abbrev']=='Sun','Weekday_flag']='Weekend'
# Generando Week_num_in_year
Time_dim_df['Week_num_in_year']=pd.PeriodIndex(Time_dim_df['Full_date']).weekofyear
# Generando Week_begin_date
Time_dim_df['Week_begin_date']=Time_dim_df['Full_date']
for index, row in Time_dim_df.iterrows():
    # obteniendo Day_of_week
    dayofweek=row['Day_of_week']
    #Calculando fecha de inicio de semana
    Time_dim_df.iloc[index,9]=row['Week_begin_date']+timedelta(days=1)-timedelta(days=dayofweek)
# Generando Week_begin_date_key
Time_dim_df['Week_begin_date_key']=(pd.DatetimeIndex(Time_dim_df['Week_begin_date']).year)*10000+(pd.DatetimeIndex(Time_dim_df['Week_begin_date']).month)*100+pd.DatetimeIndex(Time_dim_df['Week_begin_date']).day
# Generando Month
Time_dim_df['Month_']=pd.DatetimeIndex(Time_dim_df['Full_date']).month
# Generando Month_name
Time_dim_df['Month_name']=pd.DatetimeIndex(Time_dim_df['Full_date']).month_name()
# Generando Month_abbrev
Time_dim_df['Month_abbrev']=Time_dim_df['Month_name'].str.slice(0,3)
# Generando Quarter
Time_dim_df['Quarter_']=pd.DatetimeIndex(Time_dim_df['Full_date']).quarter
# Generando Year
Time_dim_df['Year_']=pd.DatetimeIndex(Time_dim_df['Full_date']).year
# Generando Yearmo
Time_dim_df['Yearmo']=(pd.DatetimeIndex(Time_dim_df['Full_date']).year)*100+(pd.DatetimeIndex(Time_dim_df['Full_date']).month)
# Generando Month_end_flag
Time_dim_df.loc[pd.DatetimeIndex(Time_dim_df['Full_date']).is_month_end==True,'Month_end_flag']='Month End'
Time_dim_df.loc[pd.DatetimeIndex(Time_dim_df['Full_date']).is_month_end==False,'Month_end_flag']='Not Month End'
# Generando Same_day_year_ago
Time_dim_df['Same_day_year_ago']=Time_dim_df['Full_date']
for index, row in Time_dim_df.iterrows():
    # Verificando año bisiesto
    if row['Same_day_year_ago'].month==2 and row['Same_day_year_ago'].day==29:
        year_adj=row['Year_']-1
        date_adj=str(year_adj)+'/03/01'
        Time_dim_df.iloc[index,18]=pd.to_datetime(date_adj)
    else:
        year_adj=row['Year_']-1
        month_adj=row['Month_']
        day_adj=row['Day_num_in_month']
        date_adj=str(year_adj)+'/'+str(month_adj)+'/'+str(day_adj)
        Time_dim_df.iloc[index,18]=pd.to_datetime(date_adj)
# Insertando Timestamp
now=datetime.now()
Time_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
Time_dim_df

# %% [markdown]
# #### Hotels_dim (Dimensión de Hoteles)

# %%
# Copiando dataframe con datos de Hoteles
Hotels_dim_df=hotelsdb_df
# Insertando Timestamp
now=datetime.now()
Hotels_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
# Insertando SK
Hotels_dim_df['Hotels_SK']=Hotels_dim_df.index+1
# Cambiando nombre a columnas acorde a DW
Hotels_dim_df.rename(columns={'id':'hotels_id','place':'Place'},inplace=True)
Hotels_dim_df

# %% [markdown]
# #### Users_dim (Dimensión de Users)

# %%
Users_dim_df=usersdb_df
now=datetime.now()
Users_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
# Insertando SK
Users_dim_df['Users_SK']=Users_dim_df.index+1
Users_dim_df

# %% [markdown]
# #### Airports_dim (Dimensión de Aeropuertos)

# %%
# Extrayendo aeropuertos de origen y destino en una serie de pandas
Airports_dim_df=pd.concat([flightsdb_df['origin'],flightsdb_df['destination']])
# Removiendo valores duplicados
Airports_dim_df.drop_duplicates(inplace=True)
# Conviertiendo a DataFrame
Airports_dim_df=pd.DataFrame(Airports_dim_df)
# Reiniciando index
Airports_dim_df.reset_index(inplace=True)
# Eliminando columna de index de la serie original
Airports_dim_df.drop(['index'],axis=1,inplace=True)
# Insertando timestamp
now=datetime.now()
Airports_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
# Renombrando columnas para coincidir con atributos de DW
Airports_dim_df.rename(columns={0:'airport'},inplace=True)
# Insertando SK
Airports_dim_df['Airport_SK']=Airports_dim_df.index+1
Airports_dim_df

# %% [markdown]
# #### Flight_type_dim (Dimensión de Flight_type)

# %%
# Extrayendo tipos de vuelo en DataFrame
Flight_type_dim_df=pd.DataFrame(flightsdb_df['flight_type'])
# Removiendo valores duplicados
Flight_type_dim_df.drop_duplicates(inplace=True)
# Reiniciando index
Flight_type_dim_df.reset_index(inplace=True)
# Eliminando columna de index del DataFrame original
Flight_type_dim_df.drop(['index'],axis=1,inplace=True)
# Insertando timestamp
now=datetime.now()
Flight_type_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
# Insertando SK
Flight_type_dim_df['Flight_type_SK']=Flight_type_dim_df.index+1
Flight_type_dim_df

# %% [markdown]
# #### Agency_dim (Dimensión de Agency)

# %%
# Extrayendo tipos de vuelo en DataFrame
Agency_dim_df=pd.DataFrame(flightsdb_df['agency'])
# Removiendo valores duplicados
Agency_dim_df.drop_duplicates(inplace=True)
# Reiniciando index
Agency_dim_df.reset_index(inplace=True)
# Eliminando columna de index del DataFrame original
Agency_dim_df.drop(['index'],axis=1,inplace=True)
# Insertando timestamp
now=datetime.now()
Agency_dim_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')
# Insertando SK
Agency_dim_df['Agency_SK']=Agency_dim_df.index+1
Agency_dim_df

# %%


# %% [markdown]
# ### Creando Tablas de Hechos

# %%
# Copiando tabla de estadias
Stays_facts_df=staysdb_df

# Join con dimensión de usuarios
## Creando DataFrame temporal que relaciona usuarios con viajes
users_dim_travels=Users_dim_df.merge(travelsdb_df,left_on='id',right_on='user_id')

users_dim_travels.rename(columns={'id_y':'travel_id'},inplace=True)
Stays_facts_df=Stays_facts_df.merge(users_dim_travels,left_on='travel_id',right_on='travel_id')
## Eliminando columnas no necesarias
Stays_facts_df.drop(['id_x','name_','company','age','gender','Timestamp','user_id'],axis=1,inplace=True)

# Join con dimensión de hoteles
Stays_facts_df=Stays_facts_df.merge(Hotels_dim_df,left_on='hotel_id',right_on='hotels_id')

## Eliminando columnas no necesarias
Stays_facts_df.drop(['hotel_id_x','hotels_id','hotel_id_y','travel_id_y','Timestamp'],axis=1,inplace=True)

# Join con dimensión de tiempo
Stays_facts_df=Stays_facts_df.merge(Time_dim_df,left_on='check_in',right_on='Full_date')
## Eliminando columnas no necesarias
Stays_facts_df.drop(['check_in','Full_date','Day_of_week','Day_num_in_month','Day_num_overall','Day_name','Day_abbrev','Weekday_flag','Week_num_in_year','Week_begin_date','Week_begin_date_key','Month_','Month_name','Month_abbrev','Quarter_','Year_','Yearmo','Month_end_flag','Same_day_year_ago','Timestamp'],axis=1,inplace=True)




# Agregando Timestamp
now=datetime.now()
Stays_facts_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')

# Agregando SK
Stays_facts_df['Stays_SK']=Stays_facts_df.index+1

# Renombrando columnas para coincidir con atributos de DW
Stays_facts_df.rename(columns={'travel_id_x':'travels_id','days_':'days','id':'stays_id','Hotels_SK':'hotel_SK','Date_key':'check_in'},inplace=True)


Stays_facts_df


# %%
Stays_facts_df

# %% [markdown]
# #### Flights_facts (Tabla de hechos de Flights)

# %%
# Copiando tabla de vuelos
Flights_facts_df=flightsdb_df

# Join con dimensión de usuarios
Flights_facts_df=Flights_facts_df.merge(users_dim_travels,left_on='travel_id',right_on='travel_id')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['id_x','name_','company','age','gender','Timestamp','user_id'],axis=1,inplace=True)

# Join con dimensión de aeropuertos
Flights_facts_df=Flights_facts_df.merge(Airports_dim_df,left_on='origin',right_on='airport')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['origin','airport','Timestamp'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Flights_facts_df.rename(columns={'id':'flights_id','Airport_SK':'Origin_SK'},inplace=True)

# Join con dimensión de aeropuertos
Flights_facts_df=Flights_facts_df.merge(Airports_dim_df,left_on='destination',right_on='airport')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['destination','airport','Timestamp'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Flights_facts_df.rename(columns={'Airport_SK':'Destination_SK'},inplace=True)

# Join con dimensión de Flight_type
Flights_facts_df=Flights_facts_df.merge(Flight_type_dim_df,left_on='flight_type',right_on='flight_type')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['flight_type','Timestamp'],axis=1,inplace=True)

# Join con dimensión de Agency
Flights_facts_df=Flights_facts_df.merge(Agency_dim_df,left_on='agency',right_on='agency')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['agency','Timestamp'],axis=1,inplace=True)

# Join con dimensión de tiempo
Flights_facts_df=Flights_facts_df.merge(Time_dim_df,left_on='departure_date',right_on='Full_date')
## Eliminando columnas no necesarias
Flights_facts_df.drop(['departure_date','Full_date','Day_of_week','Day_num_in_month','Day_num_overall','Day_name','Day_abbrev','Weekday_flag','Week_num_in_year','Week_begin_date','Week_begin_date_key','Month_','Month_name','Month_abbrev','Quarter_','Year_','Yearmo','Month_end_flag','Same_day_year_ago','Timestamp'],axis=1,inplace=True)

# Renombrando columnas para coincidir con atributos de DW
Flights_facts_df.rename(columns={'Date_key':'departure_date'},inplace=True)

# Agregando Timestamp
now=datetime.now()
Flights_facts_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')

# Agregando SK
Flights_facts_df['Flights_SK']=Flights_facts_df.index+1

Flights_facts_df

# %% [markdown]
# #### Travels_snaps (Tabla de hechos de snapshot de Travels)

# %%
print(Stays_facts_df)

# %%
Travels_snap_df

# %%
# Sumarizando métricas de estadías de hotel

## Métricas hotel
hotels_metrics_df=Stays_facts_df[['travels_id','Users_SK','stays_id','days_','total']].copy()

hotels_metrics_df

hotels_metrics_df=hotels_metrics_df.groupby(by='travels_id')['stays_id','days_','total'].agg({'stays_id':'count','days_':'sum','total':'sum'})
hotels_metrics_df.reset_index(inplace=True)


## Métricas de vuelos
flights_metrics_df=Flights_facts_df[['travel_id','price','time_','distance']].copy()
flights_metrics_df=flights_metrics_df.groupby(by='travel_id')['travel_id','price','time_','distance'].agg({'price':'sum','time_':'sum','distance':'sum'})
flights_metrics_df.reset_index(inplace=True)
flights_count_df=Flights_facts_df[['travel_id']].copy()

flights_count_df=pd.DataFrame(flights_count_df.groupby(by='travel_id')['travel_id'].agg('count'))
flights_count_df.rename(columns={'travel_id':'count'},inplace=True)
flights_count_df.reset_index(inplace=True)

## Fecha de inicio y fin del viaje
### Extrayendo fechas de estadías de hotel
stays_dates_df=Stays_facts_df[['travels_id','check_in']].copy()
### Renombrando columnas
stays_dates_df.rename(columns={'travels_id':'travel_id','check_in':'date'},inplace=True)
### Extrayendo fechas de vuelos
flights_dates_df=Flights_facts_df[['travel_id','departure_date']].copy()
### Renombrando columnas
flights_dates_df.rename(columns={'departure_date':'date'},inplace=True)
### Concatenando fechas de estadías de hotel y vuelos
dates_df=pd.concat([stays_dates_df,flights_dates_df])
### Ordenando por columna 'date' en forma ascendente
dates_df.sort_values(by=['date'],axis=0,ascending=True,inplace=True)
### Fecha de inicio del viaje (fecha del primer evento del viaje -estadía de hotel o vuelo-).
#### Se conserva el primer duplicado encontrado que corresponde al primer evento del viaje dado que está ordenado de forma ascendente por fecha.
start_dates_df=dates_df.drop_duplicates(subset=['travel_id'],keep='first')
### Fecha de fin de viaje (fecha del ultimo evento del viaje -estadía de hotel o vuelo-)
#### Se conserva el último duplicado encontrado que corresponde al último evento del viaje dado que está ordenado de forma ascendente por fecha.
end_dates_df=dates_df.drop_duplicates(subset=['travel_id'],keep='last')

# Copiando tabla de viajes
Travels_snap_df=travelsdb_df

# Join con users
Travels_snap_df=Travels_snap_df.merge(Users_dim_df,left_on='user_id',right_on='id',suffixes=('','_users_dim'))
## Eliminando columnas no necesarias
Travels_snap_df.drop(['user_id','id_users_dim','name_','company','age','gender','Timestamp'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'id':'Travels_ID'},inplace=True)

# Join con start_dates_df
Travels_snap_df=Travels_snap_df.merge(start_dates_df,left_on='Travels_ID',right_on='travel_id')
## Eliminando columnas no necesarias
Travels_snap_df.drop(['travel_id'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'date':'Start_Date_key'},inplace=True)

# Join con end_dates_df
Travels_snap_df=Travels_snap_df.merge(end_dates_df,left_on='Travels_ID',right_on='travel_id')

## Eliminando columnas no necesarias
Travels_snap_df.drop(['travel_id'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'date':'End_Date_key'},inplace=True)

# Join con métricas de hotel
Travels_snap_df=Travels_snap_df.merge(hotels_metrics_df,how='left',left_on='Travels_ID',right_on='travels_id')


## Eliminando columnas no necesarias
Travels_snap_df.drop(['travels_id'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'stays_id':'Hotels_count','days':'Hotels_days','total':'Hotels_cost'},inplace=True)

## Resolviendo NaNs. Se reemplazaran con 0, pues implican que en el viaje asociado no hubo estadías de hotel.
Travels_snap_df['Hotels_count']=Travels_snap_df['Hotels_count'].fillna(0)
Travels_snap_df['days_']=Travels_snap_df['days_'].fillna(0)
Travels_snap_df['Hotels_cost']=Travels_snap_df['Hotels_cost'].fillna(0)
Travels_snap_df.rename(columns={'days_':'Hotels_days'},inplace=True)

# Join con métricas de vuelos
Travels_snap_df=Travels_snap_df.merge(flights_metrics_df,left_on='Travels_ID',right_on='travel_id')
## Eliminando columnas no necesarias
Travels_snap_df.drop(['travel_id'],axis=1,inplace=True)
## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'price':'Flights_cost','time_':'Flights_time','distance':'Flights_distance'},inplace=True)
## Resolviendo NaNs. Se reemplazaran con 0, pues implican que en el viaje asociado no hubo vuelos.
Travels_snap_df['Flights_cost']=Travels_snap_df['Flights_cost'].fillna(0)
Travels_snap_df['Flights_time']=Travels_snap_df['Flights_time'].fillna(0)
Travels_snap_df['Flights_distance']=Travels_snap_df['Flights_distance'].fillna(0)


# Join con recuento de vuelos
Travels_snap_df=Travels_snap_df.merge(flights_count_df,left_on='Travels_ID',right_on='travel_id')

## Renombrando columnas para coincidir con atributos de DW
Travels_snap_df.rename(columns={'count':'Flights_count'},inplace=True)
## Resolviendo NaNs. Se reemplazaran con 0, pues implican que en el viaje asociado no hubo vuelos.
Travels_snap_df['Flights_count']=Travels_snap_df['Flights_count'].fillna(0)
Travels_snap_df.drop(['travel_id'],axis=1,inplace=True)
# Insertando Timestamp
now=datetime.now()
Travels_snap_df['Timestamp']=now.strftime('%Y-%m-%d %H:%M:%S')

Travels_snap_df

# %%
flights_metrics_df

# %%
hotels_metrics_df

# %% [markdown]
# #### Verificamos Instancias de RDS disponibles

# %%
rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")

# %% [markdown]
# #### Creación de Servicio RDS

# %%
rdsIdentifier = 'viajesdw'

# %% [markdown]
# #### Obtenemos URL del HOST

# %%
try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS_DW', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername=config.get('RDS_DW', 'DB_USER'),
            MasterUserPassword=config.get('RDS_DW', 'DB_PASSWORD'),
            Port=int(config.get('RDS_DW', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")

# %% [markdown]
# #### Conexión a Base de Datos desde Python

# %%
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

# %% [markdown]
# ##### Conexión a Base de Datos desde Python

# %%
import create_travels_DW
import pymysql
from pymysql.constants import CLIENT

try:
    myDDL = pymysql.connect(host=RDS_HOST, user=config.get('RDS_DW', 'DB_USER'), password=config.get('RDS_DW', 'DB_PASSWORD'), client_flag = CLIENT.MULTI_STATEMENTS)

    mycursor = myDDL.cursor()

        #Lets's create a DB
    sql = '''create database if not exists travels_DW'''
    mycursor.execute(sql)
    mycursor.connection.commit()
    
    sql = 'use travels_DW'
    mycursor.execute(sql)
    mycursor.connection.commit()
    mycursor = myDDL.cursor()
    mycursor.execute(create_travels_DW.CREATE_DB)
    mycursor.connection.commit()

    mycursor.close()
    
    

    
    print("Base de Datos Creada Exitosamente")
except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
	print("Ocurrió un error al conectar: ", e)
    
myDDL

# %% [markdown]
# #### Creando SQL Driver

# %%
def insertDataToSQL(data_dict, table_name):
    mysql_driver = f"""mysql+pymysql://{config.get('RDS_DW', 'DB_USER')}:{config.get('RDS_DW', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS_DW', 'DB_PORT')}/{config.get('RDS_DW', 'DB_NAME')}"""  
    df_data = pd.DataFrame.from_records(data_dict)
    try:
          response = df_data.to_sql(table_name, mysql_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
    except Exception as ex:
          print(ex)

# %% [markdown]
# #### Insertando datos en Base de Datos

# %%
# Renombrando últimos campos
Time_dim_df.rename(columns={'Month':'Month_'},inplace=True)
Hotels_dim_df.rename(columns={'hotel_id':'name_','travel_id':'Place'},inplace=True)
Stays_facts_df.rename(columns={'days':'days_'},inplace=True)


# %%
print(Time_dim_df)

# %%
# Insertando datos en tablas de DB Viajes
# Time_dim
insertDataToSQL(Time_dim_df, 'Time_dim')
# Hotels_dim
insertDataToSQL(Hotels_dim_df, 'Hotels_dim')
# Users_dim
insertDataToSQL(Users_dim_df, 'Users_dim')
# Airports_dim
#insertDataToSQL(Airports_dim_df, 'Airports_dim')
# Flight_type_dim
insertDataToSQL(Flight_type_dim_df, 'Flight_type_dim')
# Agency_dim
insertDataToSQL(Agency_dim_df, 'Agency_dim')
# Stays_facts
insertDataToSQL(Stays_facts_df, 'Stays_facts')
# Fligths_facts
insertDataToSQL(Flights_facts_df, 'Flights_facts')
# Travels_snap
insertDataToSQL(Travels_snap_df, 'Travels_snap')

# %%
s3 = boto3.resource(
	service_name = 's3',
	region_name ='us-east-1',
	aws_access_key_id = config.get('IAM','ACCESS_KEY'),
	aws_secret_access_key = config.get('IAM','SECRET_ACCESS_KEY')
)

# %%
# Establecer el nombre del bucket y los nombres de archivo que quieres descargar
bucket_name = 'travels2023'
file_names = ['flights.csv', 'hotels.csv', 'users.csv']

# Descargar los archivos y guardarlos en un data frame separado
df1 = pd.read_csv(s3.Object(bucket_name, file_names[0]).get()['Body'], delimiter=';')
df2 = pd.read_csv(s3.Object(bucket_name, file_names[1]).get()['Body'], delimiter=';')
df3 = pd.read_csv(s3.Object(bucket_name, file_names[2]).get()['Body'], delimiter=';')

# %%
print(df1)



