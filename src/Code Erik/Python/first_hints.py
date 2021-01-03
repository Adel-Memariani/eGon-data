#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, String, Float, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

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
    
    id = Column(String, primary_key=True)
    value = Column(Float)
    
## Create table
Table_test.__table__.create(bind=engine, checkfirst=True)
