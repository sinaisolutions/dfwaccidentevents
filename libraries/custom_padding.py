
import streamlit as st

# Hide blank space

def hide_blankSpace_top():

    hide_streamlit_style = """
    <style>
        #root > div:nth-child(1) > div > div > div > div > section > div {padding-top: 0rem;}
    </style>

    """

    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

#Remove padding b/w components 
def remove_space_components():  
    padding = 0
    st.markdown(f""" <style>
        .reportview-container .main .block-container{{
            padding-top: {padding}rem;
            padding-right: {padding}rem;
            padding-left: {padding}rem;
            padding-bottom: {padding}rem;
        }} </style> """, unsafe_allow_html=True)
        
        

'''  
#Hide Menu
def hide_menu():
    st.markdown(""" <style> #MainMenu {visibility: hidden;}
    footer {visibility: hidden;} </style> """, unsafe_allow_html=True)
    
'''

"""
st.sidebar.markdown('''
# Sections
- [Section 1](#section-1)
- [Section 2](#section-2)
''', unsafe_allow_html=True)

st.header('Section 1')
st.write('''
Lorem ipsum''')
"""