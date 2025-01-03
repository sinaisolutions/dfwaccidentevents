
import streamlit as st
from pandas import DataFrame

#Jo Schedule
job_interval=st.secrets['schedule'].job_interval 
update_interval=st.secrets['schedule'].update_interval 
run_every= st.secrets['schedule']['run_every'] 
#Database
dbId=st.secrets['serverlogins'].dbname # st.secrets['database_info'].dbId 
archivedTableId=st.secrets['serverlogins'].archivedTableId #st.secrets.database_info.archivedTableId
newdataTableID=st.secrets['serverlogins'].newdataTableId # st.secrets.database_info.newdataTableId

uname= st.secrets['serverlogins'].uname #st.secrets.database_logins.uname
pwd= st.secrets['serverlogins'].pwd #st.secrets.database_logins.pwd
hostid= st.secrets['serverlogins'].serverid #st.secrets.database_logins.hostid
port=st.secrets['serverlogins'].port

#Accident Data access
data_url= st.secrets.hostdatainfo.data_url 
tkname= st.secrets.hostdatainfo.tkname 
#map infos
icon_path=  st.secrets.map.icon_path 
icon_path_major=st.secrets.map.icon_path_major
usps_zipcode= st.secrets.map.usps_zipcode 

#empty Data Frame
empty_df=DataFrame(columns=['Id', 'Event_Number', 'Type', 'Subtype', 'Severity', 'Address', 'State', 'Zip', 'Creationtime', 'Description', 'Updatetime', 'Latitude', 'Longitude'])
        
# Credit:https://clipart-library.com; src="https://i.ibb.co/sqRk5cr/red-flag-transparent-background-4.png" 
# Colision: "https://i.ibb.co/0FHWJQQ/collision1.png"
