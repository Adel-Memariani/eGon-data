# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 11:26:36 2020

@author: nada_am
"""

# import os.path
# import subprocess
from sqlalchemy import create_engine

def initdb_local():
    """ TEST: Initialize the DLR local database used for data processing. """
    
    create_engine('postgresql://oeuser:egon@10.160.180.45:65433/testdb')
    
    
    # subprocess.run(
    #     ["docker-compose", "up", "-d", "--build"],
        # cwd=os.path.dirname(__file__),
    # )
