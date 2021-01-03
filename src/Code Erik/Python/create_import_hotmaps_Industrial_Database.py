#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 10:12:41 2020

@author: Erik Maas
"""

from numpy import genfromtxt
from time import time
from sqlalchemy import Column, Integer, Float, Date, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, dtype=(str))  
    return data.tolist()

Base = declarative_base()

# Connect to local database
engine = create_engine(
    'postgresql+psycopg2://student:student@localhost:5432/local_oedb', echo=True)
session = sessionmaker(bind=engine)()

# Create new table with sqlalchemy
## Define orm-Class
class Table_test(Base):
    __tablename__ = 'test'
    __table_args__ = {'schema': 'industry'}
    
    SiteID = Column(Integer, primary_key=True)
    CompanyName = Column(String)
    SiteName = Column(String)
    Address = Column(String)
    CityCode = Column(String)
    City = Column(String)
    Country = Column(String)
    geom = Column(String)
    Subsector = Column(String)
    DataSource = Column(String)
    Emissions_ETS_2014 = Column(String)
    Emissions_EPRTR_2014 = Column(String)
    Production  = Column(String)
    Fuel_Demand = Column(String)
    Excess_Heat_100_200C = Column(String)
    Excess_Heat_200_500C = Column(String)
    Excess_Heat_500C = Column(String)
    Excess_Heat_Total = Column(String)
    
    
## Create table
Table_test.__table__.create(bind=engine, checkfirst=True)


try:
        file_name = "t.csv" # Pfad anpasssen!!!!!!!
        data = Load_Data(file_name) 
        
            
                    
                
        j=1
        for i in data:
        #    for x in i:
         #       if x=="":
          #          ="1"
           #         print(x+"wert")
            #        print(i[12])
                
            record = Table_test(**{
                'SiteID' : j,
                'CompanyName' : i[1],
                'SiteName' : i[2],
                'Address' : i[3],
                'CityCode' : i[4],
                'City' : i[5],
                'Country' : i[6],
                'geom' : i[7],
                'Subsector' : i[8],
                'DataSource' : i[9],
                'Emissions_ETS_2014' : i[10],
                'Emissions_EPRTR_2014' : i[11],
                'Production' : i[12],
                'Fuel_Demand' : i[13],
                'Excess_Heat_100_200C' : i[14],
                'Excess_Heat_200_500C' : i[15],
                'Excess_Heat_500C' : i[16],
                'Excess_Heat_Total' : i[17]
            })
            session.add(record) #Add all the records
            j=j+1

        session.commit() #Attempt to commit all the records
except:
        session.rollback() #Rollback the changes on error
        print("error")
finally:
        session.close() #Close the connection
        
t = time()
print ("Time elapsed: " + str(time() - t) + " s.") #0.091s
