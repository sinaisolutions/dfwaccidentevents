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



def download_json(json_path=gp.data_url ,app_token=gp.tkname, user_name=None,pwd=None,timeout=4):
    
    data=pd.DataFrame()
    try:
        if(app_token):
            response=requests.get(json_path+ F'?$$app_token={app_token}',timeout=timeout)  #app_token='iRokT2ZaD4LegtLA2SkSEZ3Ww'
        else:
            response=requests.get(json_path,timeout=timeout)
        if(response.status_code==200):
            download_time=datetime.datetime.now(tz=pytz.timezone('US/Central')).strftime("%m/%d/%Y-%H:%M:%S")# get download time in US/Central time zone
            data=pd.DataFrame(response.json())[['id', 'event_number', 'type', 'subtype', 'severity', 'address', 'state', 'zip', 'creationtime', 'description', 'updatetime', 'latitude', 'longitude']]
            data['metaload_us_cst']=download_time #append download time column to data 
            response.close()
            return (data,1)
        else:
            print('Received the following error code: ', response.status_code)
            response.close()
            return (data,0)  #return dataset along with download flag status
         
    except Exception as e:
        print('Could not get JSON file; see errror below')  
        print(e)  
        return(data,0)
 
    
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
    if(not st.session_state.data.empty):
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
        
    except Exception as e:
        print(e)   
   
    
@st.fragment(run_every=gp.run_every+11)
def update_variables():
    if( not st.session_state.data.empty ):  
        st.session_state.data=cast_type(st.session_state.data,Zip='int32',Latitude='float',Longitude='float')
        st.session_state.data=expand_datetime(st.session_state.data, datetime_column='Creationtime')

        #Updating Centroid
        print('Updating Centroid')
        print('Data from update_variables')
        print(st.session_state.data)
        st.session_state.centroid_xy=get_centroid(data=filter_data(st.session_state.data,zip_code=st.session_state.zipcode,severity=None), lat_col='Latitude',long_col='Longitude')

@st.fragment(run_every=gp.run_every+11)            
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
                #st.rerun()

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

def assign_color(data=None,target_col=None,outCol_name='color'):

    data.loc[data[target_col].str.contains('MAJ',regex=True),outCol_name]='#FF0000'
    data.loc[data[target_col].str.contains('MIN',regex=True),outCol_name]='#9A0EEA'
    data.loc[data[outCol_name].isnull(),outCol_name]='#0000FF'
    return data

#************************MAP MANIPULATION AND UPDATE    
## functions to design and display the map

def get_centroid(data,lat_col=None, long_col=None):
    print('Updating Centroid infos')
    if( isinstance(data,pd.DataFrame) and (not data.empty)):

        return(data[lat_col].mean(),data[long_col].mean()) # return mean for lat_col & long_col
    else:
        print ('Returning Default Centroid; dataset is empty ')
        print('zip code', st.session_state.zipcode)
        return (32.713236, -97.34276299999999)


def filter_data(data,zip_code=None,severity=None, dropdown=False):
    
    if(isinstance(data,pd.DataFrame) and (not data.empty )):
        if(zip_code and (severity is not None) ):
            return data.query(" Severity.str.lower()== @severity.lower() and Zip==@zip_code ") # and Severity.str.lower()== '%s' "%( zip_code ,severity  ))
        elif(zip_code and (severity is None) ):
            return data.query('Zip== @zip_code ')
            
        elif(zip_code is None and (severity is not None)):
            return data.query(' Severity.str.lower()== @severity.lower() ' )
        else:
            return data  # return unfiltered data

    else:
        return gp.empty_df
        

def set_icon_layer(data,icon_url=gp.icon_path,icon_height=42,icon_width=42,anchor=42,size=3,pickable=True):
    """
    Will return an empty object if data is None or empty
    """

    icon_data = {
        # Icon from Wikimedia, used the Creative Commons Attribution-Share Alike 3.0
        # Unported, 2.5 Generic, 2.0 Generic and 1.0 Generic licenses
        "url": icon_url, #Location of png icon
        "width": icon_width,
        "height": icon_height,
        "anchorY": anchor,
    }
    pd.options.mode.copy_on_write = True
    cols=list(gp.empty_df.columns)
    cols.append('icon_data')
    df_dummy=pd.DataFrame(columns=cols, index=[0])
    df_dummy['icon_data']=icon_data

    if ( isinstance(data, pd.DataFrame) and (not data.empty ) ): 

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
        #return icon_layer
    
    else:
        print('Dummy data set: ', df_dummy)
        print('Columns: ', cols)
        icon_layer = pdk.Layer(
            type="IconLayer",
            data=df_dummy,
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


@st.fragment(run_every=gp.run_every+11)
def build_map(centroid_xy, placeholder,zip_code,data,severity_minor='minor',severity_major='major',icon_url=gp.icon_path_major):
    """
    method to build and display map
    if zipcode is set, Map will only display info relevant to that zip code
    """
    if(not data.empty):
        #placeholder.text("...Updating the Map...")
        st.toast('Updating Map....')
        print('Updating the Map at (US/CST): ',datetime.datetime.now(tz=pytz.timezone('US/Central')).strftime("%m/%d/%Y-%H:%M:%S") )

        print('Centroid: ', st.session_state.centroid_xy)
        try:

            placeholder.pydeck_chart(pdk.Deck(map_style=None,
                initial_view_state=pdk.ViewState(latitude=centroid_xy[0],longitude=centroid_xy[1],
                zoom=9,
                pitch=0,
                bearing=0
            ),
            layers=[set_icon_layer(data=filter_data(data,zip_code=zip_code,severity=severity_minor)  ,size=2.5)
            , set_icon_layer(data=filter_data(data,zip_code=zip_code,severity=severity_major) , icon_url=icon_url,icon_height=40,icon_width=40,anchor=50,size=4)
            ],
            tooltip = {
            'html': '<b>Vicinity:</b> {Address} <br><b>Desc:</b> {Description}<br><b>Time:</b> {Month} {Day}, {Hour}:{Min}',
                'style': {
                    'color': 'white',
                    "backgroundColor": "steelblue"
                }
            }
            ))
        except Exception as e:
            print('Error encountered while updating the map')
            print(e)
    st.text('New Data as read by build_map')
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
