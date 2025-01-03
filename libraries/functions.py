# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import json
import requests   
        
def download_json(json_path,app_token, user_name=None,pwd=None)        :
    response=requests.get(json_path)
    
    if str(response.status_code).startswith('4'):  #Error downloading
        return (-1,'Access NOT granted')
    elif response.status_code==500:
        return(-2,'ServerError')
    else:
        
        return (response, response.json())
        
def assign_color(data=None,target_col=None,outCol_name='color'):

    data.loc[data.target_col.str.contains('MAJ',regex=True,flags=re.IGNORECASE),outCol_name]='red']
    
    return data

        
        
    