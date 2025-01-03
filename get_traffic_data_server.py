import streamlit as st
import libraries.database as db
import libraries.globalParameters as gp

st.markdown(":orange[**Getting Data From Host .....**]")


#Writing data to DB and Archiving
db.write_postgre_db() #get_data and save to db
db.write_postgre_db(timeout=10,port=gp.port)
db.archive_to_postgresdb(port=gp.port,timeout=10) #archive to db +30