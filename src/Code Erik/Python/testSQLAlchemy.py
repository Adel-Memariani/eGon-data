#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#from flask_sqlalchemy import SQLAlchemy
#from sqlalchemy import Column, String, Float, create_engine
#from sqlalchemy.orm import sessionmaker
#from sqlalchemy.ext.declarative import declarative_base

from numpy import genfromtxt
from time import time
from datetime import datetime
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
    __tablename__ = 'test'
    __table_args__ = {'schema': 'industry'}
    
    id = Column(Integer, primary_key=True)
    value = Column(String)
    
## Create table
Table_test.__table__.create(bind=engine, checkfirst=True)


try:
        file_name = "t.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name) 

        for i in data:
            record = Table_test(**{
                'id' : i[0],
                'value' : i[1]
            })
            session.add(record) #Add all the records

        session.commit() #Attempt to commit all the records
except:
        session.rollback() #Rollback the changes on error
        print("error")
finally:
        session.close() #Close the connection
        
t = time()
print ("Time elapsed: " + str(time() - t) + " s.") #0.091s


