import sqlite3 as sql
import psycopg2 as psql
import streamlit as st
import numpy as np,pandas as pd
import datetime
from . import globalParameters as gp
from . import processing as p 

if ('data' not in st.session_state):
    st.session_state.data=pd.DataFrame()#columns=['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude'])
    #st.session_state.data.columns=st.session_state.data.columns.title()

@st.fragment(run_every=gp.run_every)   
def write_postgre_db(data_url=gp.data_url,tablename=gp.newdataTableID,dbname=gp.dbId,uname=gp.uname,upwd=gp.pwd,hostname=gp.hostid,if_exist='append',chunksize=50,timeout=10,port=None):
    """
    get JSON data and save itto 'latestdata' table in DB
    """
    from sqlalchemy import create_engine
    connexion = psql.connect(
        dbname=dbname,
        user=uname,
        password=upwd,
        host=hostname, connect_timeout=timeout)
    
    data,download_flag=p.download_json(data_url)
   
    if(port):
        engine=create_engine(f'postgresql://{uname}:{upwd}@{hostname}:{port}/{dbname}',executemany_mode='values_plus_batch')
    else:
        engine=create_engine(f'postgresql://{uname}:{upwd}@{hostname}/{dbname}',executemany_mode='values_plus_batch')
    
    unique_id=['id','event_number','zip']  #primary key
    #st.write('data from write_to_db method', data)

    if((connexion is not None) and download_flag and ( not data.empty )): #asserting if download was succesful.
        #cur=connexion.cursor()
         
        try:
            connexion.cursor().execute(F'TRUNCATE TABLE  {tablename}') 
            connexion.commit()
            print("\n")
            print("@"*50)
            print('Retrieving Data from Host/Data Source')
            print("@"*50)
            data.to_sql(tablename,con=engine,if_exists=if_exist,index=False,chunksize=chunksize,method=postgres_upsert_onconflict)
            #cur.close()
            connexion.close()
            print(F"Done writing {len(data)} newest records at: ", datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S"))
            
        except Exception as error:
            print(error)
            connexion.rollback()
            connexion.close()
    else:
        print("Connexion to DB is not defined or Unable to access host to retrieve data",flush=True)

def postgres_upsert_onconflict(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


@st.fragment(run_every=gp.run_every+30)
def archive_to_postgresdb(table_name=gp.newdataTableID,archived_to_table=gp.archivedTableId,dbname=gp.dbId,uname=gp.uname,upwd=gp.pwd,hostname=gp.hostid,port=None,if_exist='append',timeout=10):
    from sqlalchemy import create_engine
    connexion = psql.connect(
        dbname=dbname,
        user=uname,
        password=upwd,
        host=hostname, connect_timeout=timeout)
    query0=F"SELECT * FROM {table_name}"

    upsert_query= F""" INSERT INTO {archived_to_table}  SELECT * from {table_name}
        ON CONFLICT (id,event_number,zip)
        DO UPDATE
        SET type=EXCLUDED.type, severity=EXCLUDED.severity, address=EXCLUDED.address, state=EXCLUDED.state,
        creationtime=EXCLUDED.creationtime, description=EXCLUDED.description,latitude=EXCLUDED.latitude,
        longitude=EXCLUDED.longitude, updatetime=EXCLUDED.updatetime,
        update_count={archived_to_table}.update_count+1 , NOTES=NOW()
        """
    if(port):
        engine=create_engine(f'postgresql+psycopg2://{uname}:{upwd}@{hostname}:{port}/{dbname}',executemany_mode='values_plus_batch')
    else:
        engine=create_engine(f'postgresql+psycopg2://{uname}:{upwd}@{hostname}/{dbname}',executemany_mode='values_plus_batch')
    records=pd.read_sql(query0,con=engine)
    if( not records.empty ):

        if(connexion):
            try:
                connexion.cursor().execute(upsert_query) # will ignore records where there is duplicate keys::INSERT OR REPLACE ??
                connexion.commit()
                connexion.close() # Closing the connexion after writingto DB
                print(F"\nInsert/Update {len(records)}  record(s) in archieved table at: ", datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S"))
                print("$#$"*50)
            except Exception as error:
                connexion.rollback()
                connexion.close()
                print("Error while archiving to DB. No records were inserted\n")
                print(error)
                print("$#$"*50)
    else:
        print(F"\nNO record exists in {table_name}")
        print("$#$"*50)

    return None

@st.fragment(run_every=gp.run_every+6)   #read from DB into the app every run_every
def getdata_fromdb(tablename=gp.newdataTableID,dbname=gp.dbId,uname=gp.uname,upwd=gp.pwd,hostname=gp.hostid,port=None,chunksize=50,timeout=10):
    """
    read data from 'latestdata' table in DB
    update st.session_state.data value for the map
    """
    from sqlalchemy import create_engine
    data_new=pd.DataFrame(columns=['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude'])
    connexion = psql.connect(
        dbname=dbname,
        user=uname,
        password=upwd,
        host=hostname, connect_timeout=timeout)
    if(port):
        engine=create_engine(f'postgresql://{uname}:{upwd}@{hostname}:{port}/{dbname}',executemany_mode='values_plus_batch')
    else:
        engine=create_engine(f'postgresql://{uname}:{upwd}@{hostname}/{dbname}',executemany_mode='values_plus_batch')
   
    if(connexion is not None): #asserting if connexion is defined
        try: 
            df_list=[]  
            for chunk in pd.read_sql(F'SELECT * FROM {tablename}',con=engine,chunksize=chunksize):
                df_list.append(chunk)
            data_new=pd.concat(df_list)
            if(not data_new.empty): #updatewebapp session state if new data exists
                st.session_state.data=data_new
                st.session_state.data.columns=st.session_state.data.columns.str.title()
                print("-"*50)
                print(F'Retrieved {len(st.session_state.data)} records from DB at: ',datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S"))
                #print(st.session_state.data)
                print("-"*50)
            
                

        except Exception as error:
            print(error)
    else:
        print('\nConnection not defined/Unable to connect to datase for reading; potential TimeOut Error')

        #return data_new   #will be empty data frame if Exception is trown out

"""
from sqlalchemy import URL

url_object = URL.create(
    "postgresql+pg8000",
    username="dbuser",
    password="kx@jj5/g",  # plain (unescaped) text
    host="pghost10",
    database="appdb",
)
"""