#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 12:22:21 2020

@author: student
"""
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
    __tablename__ = 'test_demand'     #'GeoreferenzierungIndustriestandorte'
    __table_args__ = {'schema': 'industry'}
    
    id = Column(Integer, primary_key=True)
    DEF01= Column(Float)
    DEF02= Column(Float)
    DEF03= Column(Float)
    DEF04= Column(Float)
    DEF05= Column(Float)
    DEF06= Column(Float)
    DEF07= Column(Float)
    DEF08= Column(Float)
    DEF09= Column(Float)
    DEF0A= Column(Float)
    DEF0B= Column(Float)
    DEF0C= Column(Float)
    DEF0D= Column(Float)
    DEF0E= Column(Float)
    DEF0F= Column(Float)
    DE600= Column(Float)
    DE911= Column(Float)
    DE912= Column(Float)
    DE913= Column(Float)
    DE914= Column(Float)
    DE916= Column(Float)
    DE917= Column(Float)
    DE918= Column(Float)
    DE91A= Column(Float)
    DE91B= Column(Float)
    DE91C= Column(Float)
    DE929= Column(Float)
    DE922= Column(Float)
    DE923= Column(Float)
    DE925= Column(Float)
    DE926= Column(Float)
    DE927= Column(Float)
    DE928= Column(Float)
    DE931= Column(Float)
    DE932= Column(Float)
    DE933= Column(Float)
    DE934= Column(Float)
    DE935= Column(Float)
    DE936= Column(Float)
    DE937= Column(Float)
    DE938= Column(Float)
    DE939= Column(Float)
    DE93A= Column(Float)
    DE93B= Column(Float)
    DE941= Column(Float)
    DE942= Column(Float)
    DE943= Column(Float)
    DE944= Column(Float)
    DE945= Column(Float)
    DE946= Column(Float)
    DE947= Column(Float)
    DE948= Column(Float)
    DE949= Column(Float)
    DE94A= Column(Float)
    DE94B= Column(Float)
    DE94C= Column(Float)
    DE94D= Column(Float)
    DE94E= Column(Float)
    DE94F= Column(Float)
    DE94G= Column(Float)
    DE94H= Column(Float)
    DE501= Column(Float)
    DE502= Column(Float)
    DEA11= Column(Float)
    DEA12= Column(Float)
    DEA13= Column(Float)
    DEA14= Column(Float)
    DEA15= Column(Float)
    DEA16= Column(Float)
    DEA17= Column(Float)
    DEA18= Column(Float)
    DEA19= Column(Float)
    DEA1A= Column(Float)
    DEA1B= Column(Float)
    DEA1C= Column(Float)
    DEA1D= Column(Float)
    DEA1E= Column(Float)
    DEA1F= Column(Float)
    DEA22= Column(Float)
    DEA23= Column(Float)
    DEA24= Column(Float)
    DEA2D= Column(Float)
    DEA26= Column(Float)
    DEA27= Column(Float)
    DEA28= Column(Float)
    DEA29= Column(Float)
    DEA2A= Column(Float)
    DEA2B= Column(Float)
    DEA2C= Column(Float)
    DEA31= Column(Float)
    DEA32= Column(Float)
    DEA33= Column(Float)
    DEA34= Column(Float)
    DEA35= Column(Float)
    DEA36= Column(Float)
    DEA37= Column(Float)
    DEA38= Column(Float)
    DEA41= Column(Float)
    DEA42= Column(Float)
    DEA43= Column(Float)
    DEA44= Column(Float)
    DEA45= Column(Float)
    DEA46= Column(Float)
    DEA47= Column(Float)
    DEA51= Column(Float)
    DEA52= Column(Float)
    DEA53= Column(Float)
    DEA54= Column(Float)
    DEA55= Column(Float)
    DEA56= Column(Float)
    DEA57= Column(Float)
    DEA58= Column(Float)
    DEA59= Column(Float)
    DEA5A= Column(Float)
    DEA5B= Column(Float)
    DEA5C= Column(Float)
    DE711= Column(Float)
    DE712= Column(Float)
    DE713= Column(Float)
    DE714= Column(Float)
    DE715= Column(Float)
    DE716= Column(Float)
    DE717= Column(Float)
    DE718= Column(Float)
    DE719= Column(Float)
    DE71A= Column(Float)
    DE71B= Column(Float)
    DE71C= Column(Float)
    DE71D= Column(Float)
    DE71E= Column(Float)
    DE721= Column(Float)
    DE722= Column(Float)
    DE723= Column(Float)
    DE724= Column(Float)
    DE725= Column(Float)
    DE731= Column(Float)
    DE732= Column(Float)
    DE733= Column(Float)
    DE734= Column(Float)
    DE735= Column(Float)
    DE736= Column(Float)
    DE737= Column(Float)
    DEB11= Column(Float)
    DEB12= Column(Float)
    DEB13= Column(Float)
    DEB14= Column(Float)
    DEB15= Column(Float)
    DEB16= Column(Float)
    DEB17= Column(Float)
    DEB18= Column(Float)
    DEB19= Column(Float)
    DEB1A= Column(Float)
    DEB1B= Column(Float)
    DEB21= Column(Float)
    DEB22= Column(Float)
    DEB23= Column(Float)
    DEB24= Column(Float)
    DEB25= Column(Float)
    DEB31= Column(Float)
    DEB32= Column(Float)
    DEB33= Column(Float)
    DEB34= Column(Float)
    DEB35= Column(Float)
    DEB36= Column(Float)
    DEB37= Column(Float)
    DEB38= Column(Float)
    DEB39= Column(Float)
    DEB3A= Column(Float)
    DEB3B= Column(Float)
    DEB3C= Column(Float)
    DEB3D= Column(Float)
    DEB3E= Column(Float)
    DEB3F= Column(Float)
    DEB3G= Column(Float)
    DEB3H= Column(Float)
    DEB3I= Column(Float)
    DEB3J= Column(Float)
    DEB3K= Column(Float)
    DE111= Column(Float)
    DE112= Column(Float)
    DE113= Column(Float)
    DE114= Column(Float)
    DE115= Column(Float)
    DE116= Column(Float)
    DE117= Column(Float)
    DE118= Column(Float)
    DE119= Column(Float)
    DE11A= Column(Float)
    DE11B= Column(Float)
    DE11C= Column(Float)
    DE11D= Column(Float)
    DE121= Column(Float)
    DE122= Column(Float)
    DE123= Column(Float)
    DE124= Column(Float)
    DE125= Column(Float)
    DE126= Column(Float)
    DE127= Column(Float)
    DE128= Column(Float)
    DE129= Column(Float)
    DE12A= Column(Float)
    DE12B= Column(Float)
    DE12C= Column(Float)
    DE131= Column(Float)
    DE132= Column(Float)
    DE133= Column(Float)
    DE134= Column(Float)
    DE135= Column(Float)
    DE136= Column(Float)
    DE137= Column(Float)
    DE138= Column(Float)
    DE139= Column(Float)
    DE13A= Column(Float)
    DE141= Column(Float)
    DE142= Column(Float)
    DE143= Column(Float)
    DE144= Column(Float)
    DE145= Column(Float)
    DE146= Column(Float)
    DE147= Column(Float)
    DE148= Column(Float)
    DE149= Column(Float)
    DE211= Column(Float)
    DE212= Column(Float)
    DE213= Column(Float)
    DE214= Column(Float)
    DE215= Column(Float)
    DE216= Column(Float)
    DE217= Column(Float)
    DE218= Column(Float)
    DE219= Column(Float)
    DE21A= Column(Float)
    DE21B= Column(Float)
    DE21C= Column(Float)
    DE21D= Column(Float)
    DE21E= Column(Float)
    DE21F= Column(Float)
    DE21G= Column(Float)
    DE21H= Column(Float)
    DE21I= Column(Float)
    DE21J= Column(Float)
    DE21K= Column(Float)
    DE21L= Column(Float)
    DE21M= Column(Float)
    DE21N= Column(Float)
    DE221= Column(Float)
    DE222= Column(Float)
    DE223= Column(Float)
    DE224= Column(Float)
    DE225= Column(Float)
    DE226= Column(Float)
    DE227= Column(Float)
    DE228= Column(Float)
    DE229= Column(Float)
    DE22A= Column(Float)
    DE22B= Column(Float)
    DE22C= Column(Float)
    DE231= Column(Float)
    DE232= Column(Float)
    DE233= Column(Float)
    DE234= Column(Float)
    DE235= Column(Float)
    DE236= Column(Float)
    DE237= Column(Float)
    DE238= Column(Float)
    DE239= Column(Float)
    DE23A= Column(Float)
    DE241= Column(Float)
    DE242= Column(Float)
    DE243= Column(Float)
    DE244= Column(Float)
    DE245= Column(Float)
    DE246= Column(Float)
    DE247= Column(Float)
    DE248= Column(Float)
    DE249= Column(Float)
    DE24A= Column(Float)
    DE24B= Column(Float)
    DE24C= Column(Float)
    DE24D= Column(Float)
    DE251= Column(Float)
    DE252= Column(Float)
    DE253= Column(Float)
    DE254= Column(Float)
    DE255= Column(Float)
    DE256= Column(Float)
    DE257= Column(Float)
    DE258= Column(Float)
    DE259= Column(Float)
    DE25A= Column(Float)
    DE25B= Column(Float)
    DE25C= Column(Float)
    DE261= Column(Float)
    DE262= Column(Float)
    DE263= Column(Float)
    DE264= Column(Float)
    DE265= Column(Float)
    DE266= Column(Float)
    DE267= Column(Float)
    DE268= Column(Float)
    DE269= Column(Float)
    DE26A= Column(Float)
    DE26B= Column(Float)
    DE26C= Column(Float)
    DE271= Column(Float)
    DE272= Column(Float)
    DE273= Column(Float)
    DE274= Column(Float)
    DE275= Column(Float)
    DE276= Column(Float)
    DE277= Column(Float)
    DE278= Column(Float)
    DE279= Column(Float)
    DE27A= Column(Float)
    DE27B= Column(Float)
    DE27C= Column(Float)
    DE27D= Column(Float)
    DE27E= Column(Float)
    DEC01= Column(Float)
    DEC02= Column(Float)
    DEC03= Column(Float)
    DEC04= Column(Float)
    DEC05= Column(Float)
    DEC06= Column(Float)
    DE300= Column(Float)
    DE401= Column(Float)
    DE402= Column(Float)
    DE403= Column(Float)
    DE404= Column(Float)
    DE405= Column(Float)
    DE406= Column(Float)
    DE407= Column(Float)
    DE408= Column(Float)
    DE409= Column(Float)
    DE40A= Column(Float)
    DE40B= Column(Float)
    DE40C= Column(Float)
    DE40D= Column(Float)
    DE40E= Column(Float)
    DE40F= Column(Float)
    DE40G= Column(Float)
    DE40H= Column(Float)
    DE40I= Column(Float)
    DE803= Column(Float)
    DE804= Column(Float)
    DE80J= Column(Float)
    DE80K= Column(Float)
    DE80L= Column(Float)
    DE80M= Column(Float)
    DE80N= Column(Float)
    DE80O= Column(Float)
    DED41= Column(Float)
    DED42= Column(Float)
    DED43= Column(Float)
    DED44= Column(Float)
    DED45= Column(Float)
    DED21= Column(Float)
    DED2C= Column(Float)
    DED2D= Column(Float)
    DED2E= Column(Float)
    DED2F= Column(Float)
    DED51= Column(Float)
    DED52= Column(Float)
    DED53= Column(Float)
    DEE01= Column(Float)
    DEE02= Column(Float)
    DEE03= Column(Float)
    DEE04= Column(Float)
    DEE05= Column(Float)
    DEE07= Column(Float)
    DEE08= Column(Float)
    DEE09= Column(Float)
    DEE06= Column(Float)
    DEE0A= Column(Float)
    DEE0B= Column(Float)
    DEE0C= Column(Float)
    DEE0D= Column(Float)
    DEE0E= Column(Float)
    DEG01= Column(Float)
    DEG02= Column(Float)
    DEG03= Column(Float)
    DEG04= Column(Float)
    DEG05= Column(Float)
    DEG0N= Column(Float)
    DEG06= Column(Float)
    DEG07= Column(Float)
    DEG0P= Column(Float)
    DEG09= Column(Float)
    DEG0A= Column(Float)
    DEG0B= Column(Float)
    DEG0C= Column(Float)
    DEG0D= Column(Float)
    DEG0E= Column(Float)
    DEG0F= Column(Float)
    DEG0G= Column(Float)
    DEG0H= Column(Float)
    DEG0I= Column(Float)
    DEG0J= Column(Float)
    DEG0K= Column(Float)
    DEG0L= Column(Float)
    DEG0M= Column(Float)

    
    

    
    
    
    
    
    
    
    
    
## Create table
Table_test.__table__.create(bind=engine, checkfirst=True)

#%%
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