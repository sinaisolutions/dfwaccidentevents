# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import json, time,datetime,pytz,os
import requests 
import pydeck as pdk 
import pandas as pd,streamlit as st
#from sodapy import Socrata
#import multiprocessing 
import concurrent.futures as cf
import threading, schedule 
import sqlite3 as sql
from  . import   database as db
from . import globalParameters as gp

#from streamlit.runtime.scriptrunner import get_script_run_ctx
from streamlit.runtime.scriptrunner import add_script_run_ctx

if ('download_time' not in st.session_state):
    st.session_state.download_time=None

if ('data' not in st.session_state):
    st.session_state.data=pd.DataFrame()

if('subset_data' not in st.session_state):
    st.session_state.subset_data=pd.DataFrame()

def add_backgroung(image):
    with open(image,'rb') as img:
        encoded_str=base64.b64encode(img.read())
        
    st.markdown(
    f"""
    <style>
    .stApp{{
        background-image:url(data:image/{"png"};base64, {encoded_str.decode()});
        background-size: cover
        }}
        </style>
    """, unsafe_allow_html=True
    )



def download_json(json_path=gp.data_url ,app_token=gp.tkname, user_name=None,pwd=None):
    
    data=pd.DataFrame()
    try:
        if(app_token):
            response=requests.get(json_path+ F'?$$app_token={app_token}')  #app_token='iRokT2ZaD4LegtLA2SkSEZ3Ww'
        else:
            response=requests.get(json_path)
        if(response.status_code==200):
            download_time=datetime.datetime.now(tz=pytz.timezone('US/Central')).strftime("%m/%d/%Y-%H:%M:%S")# get download time in US/Central time zone
            data=pd.DataFrame(response.json())[['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude']]
            data['metaload_us_cst']=download_time #append download time column to data 
            return (data,1)
        else:
            print('Received the following error code: ', response.status_code)
            return (data,0)  #return dataset along with download flag status
         
    except Exception as e:
        print('Could not get JSON file; see errror below')  
        print(e)  
        return(data,0)
    
        
def append_csv(input_data,existingFile,path_file=None, chunk_size=500):
    """
    Input data: pd.DataFrame object
    existingFile:: name of the existing csv file **.csv
    path_file:: path to directory to write the file
    """
    print('Saving data to DB....')
    if(os.path.exists(path_file+existingFile)):
        file=open(path_file+existingFile,'r')
        try:
            if(len(file.read(10))>0): # if existing file not empty, append to it
                input_data.to_csv(path_file+existingFile,mode='a',index=False,header=False,chunksize=chunk_size) #append to existing :: NEED TO REMOVE DUPLICATES
        except: raise("Can't Append to file:")
    else: # if  file does not exist, create it.
        try:
            input_data.to_csv(path_file+existingFile,mode='w',index=False,header=True,chunksize=chunk_size)
        except: raise("Can't write to file:")
    
    if(len(input_data)>0): #Always update the latest dataset by overriding the existing one, if json download was successful. 
       
        input_data.to_csv(path_file+'latest_dfw_traffic.csv',mode='w',index=False,header=True,chunksize=chunk_size)
    print('Done Saving')
    
    return None
    
def change_case(data_object):
    print('Formatting Headers',datetime.datetime.now())
    data_object.columns=data_object.columns.str.title()
    

def cast_type(df,**kwargs):
    print('Casting dtypes',datetime.datetime.now())
    map_dict={}
    for key,val in kwargs.items():
        map_dict[key]=val
        
    df=df.astype(map_dict)
        
    return df
        
def expand_datetime(data, datetime_column):
    """
    method to take a date time column, expand it into 
    individual date & time attributes
    datetime_column: name of column with datetime format
    return: a df with added attributes
    """
    print('Expanding datetime attributes')
    
    if(not data.empty): #only do this if data exists in df
        data['CreationTime2']=data['Creationtime'].apply(lambda x : pd.to_datetime(x))
        data['Month']=data['CreationTime2'].apply(lambda x : x.month_name()[:3])
        data['Day']=data['CreationTime2'].apply(lambda x : x.day)
        data['Hour']=data['CreationTime2'].apply(lambda x : x.hour)
        data['Min']=data['CreationTime2'].apply(lambda x : x.minute if x.minute>9 else "0"+str(x.minute))

    return data
    
@st.fragment(run_every=gp.run_every)        
def download_data(out_data=None, json_url=gp.data_url,app_token=gp.tkname):#frequency to be set in minutes, can be a float
    if(st.session_state.data.empty):
        data=st.session_state.data.copy()
    try:
        #time.sleep(gp.run_every)
        response=download_json(json_path=json_url) #Calling download_json functions
    
        if(response.status_code==200):
            st.session_state.download_time=datetime.datetime.now(tz=pytz.timezone('US/Central')).strftime("%m/%d/%Y-%H:%M:%S")# get download time in US/Central time zone
            st.session_state.data=pd.DataFrame(response.json())[['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude']]
            st.session_state.data.columns=st.session_state.data.columns.str.title()
            st.session_state.data['MetaLoad(CST)']=st.session_state.download_time #append download time column to data 
        else:
            print(F'ERROR Downloading. Received Code: {response.status_code}' )

            st.session_state.data=data
        
        if(out_data):out_data.traffic=st.session_state.data
        
        
        
        #st.rerun()

    except Exception as e:
        print(e)   
   
    
@st.fragment(run_every=gp.run_every+5)
def update_variables():
    if( not st.session_state.data.empty ):  
        st.session_state.data=cast_type(st.session_state.data,Zip='int32',Latitude='float',Longitude='float')
        st.session_state.data=expand_datetime(st.session_state.data, datetime_column='Creationtime')

        #Updating Centroid
        print('Updating Centroid')
        st.session_state.centroid_xy=get_centroid(data=filter_data(st.session_state.data,zip_code=st.session_state.zipcode,severity=None), lat_col='Latitude',long_col='Longitude')

@st.fragment(run_every=gp.run_every+7)            
def update_displays(zoom_col):
    """
    Method to display/update variables/zip-code on the screen
    """
    zoom_col1=st.container()
    with zoom_col1:
        up_date=st.empty()
        st.divider()
        zip_loc=st.empty()
        st.divider()
        holder_table=st.empty() 
         
        if(st.session_state.download_time):
            dtime=datetime.datetime.strptime(st.session_state.download_time,"%m/%d/%Y-%H:%M:%S" ) # + datetime.timedelta(minutes=6)
            up_date.markdown(F' :blue[Last Downloaded : ] { dtime.strftime("%m/%d/%Y-%H:%M:%S")}')
        
        if (not st.session_state.data.empty):
            #*** UPDATE FILTER BY ZIPCODE DROP DOWN
            zip_loc.selectbox(':orange[Filter by Zip Code]',options=sorted(st.session_state.data.Zip.unique()),index=None,placeholder='Pick  a zip code',key='zipcode')

            st.session_state.subset_data=filter_data(st.session_state.data,zip_code=st.session_state.zipcode,severity=None,dropdown=True)
            if(st.session_state.zipcode):
                holder_table.data_editor(st.session_state.subset_data[['Zip','Description','Address']].rename(columns={'Address':"Location(Approx)",'Description':'Note'}),hide_index=True,disabled=True,use_container_width=True)
          

        #st.text('Data within update_displays::')
        #st.write('Raw: ',st.session_state.data)
        #st.write('Subset:',st.session_state.subset_data) 
#-------------------------------------------------------------                   
@st.fragment(run_every=gp.run_every)   
def mp_download_data(out_data=None, json_url=gp.data_url,app_token=gp.tkname):#frequency to be set in minutes, can be a float
    data=None # place holder for data
    
    #ex=cf.ThreadPoolExecutor(max_workers=2)
    response=download_json(json_path=json_url) #Calling download_json functions
    if(response.status_code==200):
        download_time=datetime.datetime.now(tz=pytz.timezone('US/Central')).strftime("%m/%d/%Y-%H:%M:%S")# get download time in US/Central time zone
        data=pd.DataFrame(response.json())[['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude']]
        data.columns=data.columns.str.title()
        data['MetaLoad(CST)']=download_time #append download time to data 
    else:
        print('ERROR Downloading. Received Code:\n ' + str(response.status_code) )
    print(F'downloaded {len(data)} records at {download_time}')
    if(out_data):out_data.traffic=data
   
    return data   
    #print(out_data.traffic)
#**** Unique function to download and save data to  
def get_data_and_save(url=gp.data_url , saveDatatoPath=gp.path_file,connexion=None ):
    print('Downloding Data', datetime.datetime.now())
    new_data=download_data(json_url=url)
    print("number of records:",len(new_data) )
    if(not new_data.empty):
        change_case(new_data)  #String to Title Style formatting
        df=expand_datetime(data=new_data, datetime_column='Creationtime')
        df=cast_type(df=new_data,Zip='int32',Latitude='float',Longitude='float')
        db.write_to_db(data=df,table_name=gp.newTableId,keys='Id',if_exists='replace') # write newest data to sqlite DB
        db.archive_to_db(table_name=gp.newTableId, archived_table=gp.archivedTableId) #archive to db
        #append_csv(input_data=df,existingFile='dfw_traffic.csv',path_file=saveDatatoPath)
        print('Done writing to DB at(US/Central) ',datetime.datetime.now(tz=pytz.timezone('US/Central')) )
    else: print('No Record Downloaded.\n')        
   
def assign_color(data=None,target_col=None,outCol_name='color'):

    data.loc[data[target_col].str.contains('MAJ',regex=True),outCol_name]='#FF0000'
    data.loc[data[target_col].str.contains('MIN',regex=True),outCol_name]='#9A0EEA'
    data.loc[data[outCol_name].isnull(),outCol_name]='#0000FF'
    return data




class update_df:
    """
    class to update a pd.DataFrame(old_df) using data from new df
    old_df: baseline df; dataset with records that need to be updated
    new_df: df with new infos that needs to be fed to old_df
    key_colName: name of column to use as key to identify same records in both df. column must exist in both df
    """

    def __init__(self,old_df,new_df,key_colName):
    
        self.old_df=old_df
        self.new_df=new_df
        self.key=key_colName
        self.common_val=None
        self.index=None
        self.new_index=None
        if(not isinstance(self.key,str)):
            raise(ValueError('\targ key_colName must be a string, invalid dataType was passed'))
        if(self.key not in set(self.old_df.columns).intersection(self.new_df.columns)):
            raise(ValueError(F'\t{self.key} is not a common attribute of both datasets') )
    
      
    def get_indexes(self):
        """
        retrieve corresponding indexes from old_df if common keys is found
        """
        self.index=self.old_df[ self.old_df[self.key].isin(self.new_df[self.key])].index  #get indexes of records to be updated
        self.new_index=self.new_df[ self.new_df[self.key].isin(self.old_df[self.key])].index  #
        
    def update_old_df(self,column_to_update=None):
        """
        update records if matches are found
        """
    
        if(len(self.index)):
            if(column_to_update):
                self.old_df.loc[self.index,column_to_update]=self.new_df.loc[self.new_index][column_to_update].values
                
            else:
                self.old_df.loc[self.index,:]=self.new_df.loc[self.new_index].values
            
    
    def append_new_records(self):
        """
        will append new records to old records:: this method can be call directly after initializing the class
        """
        self.old_df.append(self.new_df[ ~self.new_df[self.key].isin(self.old_df[self.key])]) # will append old rows in new_df not in old_df
        
        return self.old_df
         

    

#************************MAP MANIPULATION AND UPDATE    
## functions to design and display the map

def get_centroid(data,lat_col=None, long_col=None):
    print('Updating Centroid infos')

    return(data[lat_col].mean(),data[long_col].mean()) # return mean for lat_col & long_col


def filter_data(data,zip_code=None,severity=None, dropdown=False):
    
    if(isinstance(data,pd.DataFrame) and not data.empty):
        if(zip_code and (severity is not None) ):
            return data.query(" Severity.str.lower()== @severity.lower() and Zip==@zip_code ") # and Severity.str.lower()== '%s' "%( zip_code ,severity  ))
        elif(zip_code and (severity is None) ):
            return data.query('Zip== @zip_code ')
            
        elif(zip_code is None and severity):
            return data.query(' Severity.str.lower()== @severity.lower() ' )
        else:
            if(dropdown): 
                return pd.DataFrame(columns=['Id', 'Event_Number', 'Type', 'Subtype', 'Severity', 'Address', 'State', 'Zip', 'Creationtime', 'Description', 'Updatetime', 'Latitude', 'Longitude'])
            else: return data
    else:
        return pd.DataFrame(columns=['Id', 'Event_Number', 'Type', 'Subtype', 'Severity', 'Address', 'State', 'Zip', 'Creationtime', 'Description', 'Updatetime', 'Latitude', 'Longitude'])
        

def set_icon_layer(data,icon_url=gp.icon_path,icon_height=42,icon_width=42,anchor=42,size=3,pickable=True):

    if ( not data.empty ): 
        icon_data = {
            # Icon from Wikimedia, used the Creative Commons Attribution-Share Alike 3.0
            # Unported, 2.5 Generic, 2.0 Generic and 1.0 Generic licenses
            "url": icon_url, #Location of png icon
            "width": icon_width,
            "height": icon_height,
            "anchorY": anchor,
        }
        pd.options.mode.copy_on_write = True 
        data.loc[:,"icon_data"] = None
        for i in data.index:
            data.at[i,"icon_data"]= icon_data
     
              
        icon_layer = pdk.Layer(
            type="IconLayer",
            data=data,
            get_icon="icon_data",
            get_size=size,
            size_scale=10,
            get_position=["Longitude", "Latitude"],
            pickable=pickable,
            auto_highlight=True,
            extruded=True,
        )
        return icon_layer
    
def set_scatterPlotLayer(data,radius=300,color=[153,51,255]):

    layer=pdk.Layer(
               'ScatterplotLayer',
               data=data,
               get_position=['Longitude', 'Latitude'],
               get_radius=radius,
               pickable=True,
               tooltip=True,
               get_fill_color =color,
               auto_highlight=True,
          
          )
    return layer        
            
def set_textLayer(data,text_column='Severity',text_anchor='middle',text_alignment='bottom',color=[0,0,0],size=9,orientation=0):

    pdk.Layer(
               'TextLayer',
               data=data,
               get_position=['Longitude', 'Latitude'],
               get_text=text_column,
               get_size=size,
               pickable=True,
               get_angle=orientation,
               get_color=color,#[0, 100, 205],
               get_text_anchor=String(text_anchor),
               get_alignment_baseline=String(text_alignment),
            )


@st.fragment(run_every=gp.run_every+10)
def build_map(centroid_xy, placeholder,zip_code,data,severity_minor='minor',severity_major='major',icon_url="https://i.ibb.co/sqRk5cr/red-flag-transparent-background-4.png"):
    """
    method to build and display map
    if zipcode is set, Map will only display info relevant to that zip code
    """
    if(not data.empty):
        #placeholder.text("...Updating the Map...")
        st.toast('Updating Map....')
    
        placeholder.pydeck_chart(pdk.Deck(map_style=None,
            initial_view_state=pdk.ViewState(latitude=centroid_xy[0],longitude=centroid_xy[1],
            zoom=9,
            pitch=0,
            bearing=0
        ),
        layers=[
        set_icon_layer(data=filter_data(data,zip_code=zip_code,severity=severity_major) , icon_url=icon_url,icon_height=40,icon_width=40,anchor=50,size=4)
        ,set_icon_layer(data=filter_data(data,zip_code=zip_code,severity=severity_minor)  ,size=2.5),],
        tooltip = {
        'html': '<b>Vicinity:</b> {Address} <br><b>Desc:</b> {Description}<br><b>Time:</b> {Month} {Day}, {Hour}:{Min}',
            'style': {
                'color': 'white',
                "backgroundColor": "steelblue"
            }
        }
        ))
    st.text('New Data')
    st.dataframe(data)

def run_ProcesInbackground_continuously(interval=1):
    """Continuously run, while executing pending jobs at each
    elapsed time interval.
    @return cease_continuous_run: threading. Event which can
    be set to cease continuous run. Please note that it is
    *intended behavior that run_continuously() does not run
    missed jobs*. For example, if you've registered a job that
    should run every minute and you set a continuous run
    interval of one hour then your job won't be run 60 times
    at each interval but only once.
    """
    #cease_continuous_run = threading.Event()
    cease_continuous_run = multiprocessing.Event()

    class ScheduleThread(multiprocessing.Process):
        @classmethod
        def run(cls):
            while not cease_continuous_run.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    continuous_thread.start()
    
    return cease_continuous_run
    

def run_ThreadInbackground_continuously(interval=1):
    """Continuously run, while executing pending jobs at each
    elapsed time interval.
    @return cease_continuous_run: threading. Event which can
    be set to cease continuous run. Please note that it is
    *intended behavior that run_continuously() does not run
    missed jobs*. For example, if you've registered a job that
    should run every minute and you set a continuous run
    interval of one hour then your job won't be run 60 times
    at each interval but only once.
    """
    cease_continuous_run = threading.Event()
 
    class ScheduleThread(threading.Thread):
        @classmethod
        def run(cls):
            while not cease_continuous_run.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    add_script_run_ctx(continuous_thread)
    continuous_thread.start()
    
    return cease_continuous_run
