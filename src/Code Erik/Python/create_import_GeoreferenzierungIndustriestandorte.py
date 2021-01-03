#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 13:11:33 2020

@author: Erik Maas
"""
from numpy import genfromtxt
from time import time
from sqlalchemy import Column, Integer, Float, Date, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, dtype=(str))  #dtype=[('myint','i8'),('mystring','S5')])
    return data.tolist()

Base = declarative_base()

# Connect to local database
engine = create_engine(
    'postgresql+psycopg2://student:student@localhost:5432/local_oedb', echo=True)
session = sessionmaker(bind=engine)()

# Create new table with sqlalchemy
## Define orm-Class
class Table_test(Base):
    __tablename__ = 'test1'     #'GeoreferenzierungIndustriestandorte'
    __table_args__ = {'schema': 'industry'}
    
    id = Column(Integer, primary_key=True)
    Application = Column(String)
    Plant = Column(String)
    LandkreisNr = Column(Integer)
    AnnualTonnes = Column(Float)
    CapacityOrProduction = Column(String)
    Latitude = Column(Integer)
    Longitude = Column(Integer)
    Nr1 = Column(Integer)
    Nr2 = Column(Integer)
    Comment1 = Column(String)
    Comment2 = Column(String)
    Comment3 = Column(String)
    Comment4 = Column(String)
    Comment5 = Column(String)
    Comment6 = Column(String)
    
## Create table
Table_test.__table__.create(bind=engine, checkfirst=True)


try:
        file_name = "sources/GeoreferenzierungIndustriestandorte (copy).csv" # Pfad anpasssen!!!!!!!
        data = Load_Data(file_name) 
        j=1
        for i in data:
            record = Table_test(**{
                'id' : j,
                'Application' : i[0],
                'Plant' : i[1],
                'LandkreisNr' : i[2],
                'AnnualTonnes' : i[3],
                'CapacityOrProduction' : i[4],
                'Latitude' : i[5],
                'Longitude' : i[6],
                'Nr1' : i[7],
                'Nr2' : i[8],
                'Comment1' : i[9],
                'Comment2' : i[10],
                'Comment3' : i[11],
                'Comment4' : i[12],
                'Comment5' : i[13],
                'Comment6' : i[14],
            })
            j=j+1
            session.add(record) #Add all the records

        session.commit() #Attempt to commit all the records
except:
        session.rollback() #Rollback the changes on error
        print("error")
finally:
        session.close() #Close the connection
        
t = time()
print ("Time elapsed: " + str(time() - t) + " s.") #0.091s