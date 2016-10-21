#!/usr/bin/python
# -*- coding: utf-8 -*-

from PyOPC.OPCContainers import *
from PyOPC.XDAClient import XDAClient

import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import os

import time, datetime

from sqlalchemy import create_engine # database connection

import address  #geheime Serveradresse

def print_options((ilist,options)):
    print ilist; print options; print

xda = XDAClient(OPCServerAddress=address.address,
                ReturnErrorText=True)

try:
  xda.GetStatus()
  print ("Serverzugriff erfolgt")
except:
  print("Kein Serverzugriff")
  exit()

disk_engine = create_engine('sqlite:///egneos2.db')

lastTS=pd.read_sql_query('SELECT max("index") FROM Wecstd_Raw LIMIT 1', disk_engine).values[0][0]
lastTS=datetime.datetime.strptime(lastTS, "%Y-%m-%d %H:%M:%S.%f")
lastTS

jetzt=xda.Read([ItemContainer(ItemName='Loc/Wec/Plant2/P')],ReturnItemTime=True)[0][0].Timestamp

first_path='Loc/Wec/Plant2'
Folders=['Log/Wecstd','Log/T101a1','Log/T101a2','Ava']

Wecstd_descRawRepDay=[
u'Timestamp',   
u'Source',    
u'Anzahl der Abtastwerte',
u'Mittlere Windgeschw. [m/s]',
u'Maximale Windgeschw. [m/s]',
u'Minimale Windgeschw. [m/s]',
u'Mittlere Rotordrehzahl [U/s]',
u'Maximale Rotordrehzahl [U/s]',
u'Minimale Rotordrehzahl [U/s]',
u'Mittlere Leistung [kW]',
u'Maximale Leistung [kW]',
u'Minimale Leistung [kW]',
u'Gondelposition [º]',
u'Betriebsstunden [h]',
u'Produzierte Energie [kWh]',
u'Produktionsminuten [min]',
u'Mittlere Blindleistung [kVar]',
u'Maximale Blindleistung [kVar]',
u'Minimale Blindleistung [kVar]',
u'Mittelwert der theoretisch verfügbaren Wirkleistung im Wind [kW]',
u'Mittelwert der technisch verfügbaren Wirkleistung [kW]',
u'Mittelwert der maximal verfügbaren Wirkleistung begrenzt durch höhere Gewalt [kW]',
u'Mittelwert der verfügbaren Wirkleistung begrenzt durch externe Vorgaben [kW]',
u'Mittelwert Blattwinkel über A, B, C [°]','?1','?2','?3','?4','?5','?6','?7','?8'
]

Wecstd_descWeekMonthYear=[
u'Timestamp',    
u'Source', 
u'Anzahl der Abtastwerte',
u'Mittlere Windgeschw. [m/s]',
u'Maximale Windgeschw. [m/s]',

u'Mittlere Rotordrehzahl [U/s]',
u'Maximale Rotordrehzahl [U/s]',

u'Mittlere Leistung [kW]',
u'Maximale Leistung [kW]',


u'Betriebsstunden [h]',
u'Produzierte Energie [kWh]',
u'Produktionsminuten [min]',
u'Mittlere Blindleistung [kVar]',
u'Maximale Blindleistung [kVar]',
u'Minimale Blindleistung [kVar]',
u'Mittelwert der theoretisch verfügbaren Wirkleistung im Wind [kW]',
u'Mittelwert der technisch verfügbaren Wirkleistung [kW]',
u'Mittelwert der maximal verfügbaren Wirkleistung begrenzt durch höhere Gewalt [kW]',
u'Mittelwert der verfügbaren Wirkleistung begrenzt durch externe Vorgaben [kW]'
]

Wecstd_descRawRepDayshort=[
u'index',   
u'Source',    
u'Values',
u'MitVwind',
u'MaxVwind',
u'MinVwind',
u'MitNRotor',
u'MaxNRotor',
u'MinNRotor',
u'MitP',
u'MaxP',
u'MinP',
u'GoPos',
u'Hour',
u'Wexp',
u'Minutes',
u'MitQ',
u'MaxQ',
u'MinQ',
u'PavaVwind',
u'PavaTech',
u'PavaForceM',
u'PavaExtern',
u'Pitch','?1','?2','?3','?4','?5','?6','?7','?8'
]

Wecstd_descWeekMonthYearshort=[
u'index',   
u'Source',    
u'numberOfValues',
u'MitVwind',
u'MaxVwind',

u'MitNRotor',
u'MaxNRotor',

u'MitP',
u'MaxP',


u'Hour',
u'Wexp',
u'Minutes',
u'MitQ',
u'MaxQ',
u'MinQ',
u'PavaVwind',
u'PavaTech',
u'PavaForceM',
u'PavaExtern'
]

T101a1_descshort=[u'index', u'Source', 'Values','T1','T2','T3','T4','T5','T6','T7','T8','T9','T10',
                               'T11','T12','T13','T14','T15','T16','T17','T18','T19','T20',
                                'T21','T22','T23','T24','T25']    

T101a2_descshort=[u'index',u'Source', 'Values','T1','T2','T3','T4','T5','T6','T7','T8','T9','T10',
                               'T11','T12','T13','T14'] 

Ava_descshort=[u'index', u'Source',  u'T1_betriebsbereit','T2_Gesamtzeit','T3','T4','T5','T6']



def catchit(p,Tscales_str,numbers,descr):
    itemcollect=[]
    for i in xda.Browse(ItemName=os.path.join(p,Tscales_str),MaxElementsReturned=850)[0][:]:
        if not i.HasChildren and (i.Name.startswith("Val-") or (i.Name.startswith("T-"))):
            
            if int(i.Name.rsplit('-', 1)[1])<=numbers:
                ii = ItemContainer(ItemName=os.path.join(p,Tscales_str,i.Name))
                #print i.Name,
                itemcollect.append(ii)
    #print itemcollect
    if not itemcollect:
        return
    all=xda.Read(itemcollect,ReturnItemTime=True)
    log=[]
    x=0
    for i in all[0]:
        #i.Name=str.replace(i.ItemName,path+"/","")
        datafield=np.hstack((i.Timestamp,Tscales_str,i.Value))
        #print i.Value
        log.append(datafield)
        x=x+1
    log_df=pd.DataFrame(data=log[::-1],columns=descr)
    
    log_df.index=log_df['index']
    log_df.drop('index', axis=1, inplace=True)    
        
    return log_df    

itemcollect=[] 
for F in Folders:
    F_db=str.replace(F,"Log/","")
    p=os.path.join(first_path,F)
    print p
    for Tscales in xda.Browse(ItemName=p)[0][:]:
        if Tscales.HasChildren:
            Tscales_str=Tscales.ItemName.rsplit('/', 1)[1]
            T = ItemContainer(ItemName=os.path.join(p,Tscales.Name))
            itemcollect.append(T)
            print Tscales_str,":\t",
            try:
                lastTS=pd.read_sql_query('SELECT max("index") FROM '+F_db+"_"+Tscales_str+' LIMIT 1', disk_engine).values[0][0]
                lastTS=datetime.datetime.strptime(lastTS, "%Y-%m-%d %H:%M:%S.%f")
            except:
                lastTS=datetime.datetime(1990, 1, 1, 1, 0, 0)
            now=xda.Read([ItemContainer(ItemName='Loc/Wec/Plant2/P')],ReturnItemTime=True)[0][0].Timestamp
            print "Now: ",now
            diff=now-lastTS
            
            if (Tscales_str=='Raw') or (Tscales_str=='Rep'):
                sampletime=xda.Read([ItemContainer(ItemName=os.path.join(p,str(Tscales_str+"SmpTime")))],ReturnItemTime=True)[0][0].Value
                print sampletime,Tscales_str,os.path.join(p,Tscales_str,str(Tscales_str+"SmpTime"))
                num=(now-lastTS).total_seconds()/sampletime
                
            if (Tscales_str=='Day'):
                num=(now-lastTS).days+(now.year-lastTS.year)*365-1
                print now,lastTS,"Num: ",num
            if (Tscales_str=='Week'):    
                num=now.isocalendar()[1]-lastTS.isocalendar()[1]+(now.year-lastTS.year)*53-1
            if (Tscales_str=='Month'):
                num=(now.month-lastTS.month)+(now.year-lastTS.year)*12-1
            if (Tscales_str=='Year'):
                num=(now.year-lastTS.year)-1
            print "Num: ",num,    
            #Description selector
            if F_db=="Wecstd":
                noVal=xda.Read([ItemContainer(ItemName=os.path.join(p,Tscales_str,"NoVal"))],ReturnItemTime=True)[0][0].Value
                if (Tscales_str=='Raw') or (Tscales_str=='Rep') or (Tscales_str=='Day'):
                    descr=Wecstd_descRawRepDayshort
                else:
                    descr=Wecstd_descWeekMonthYearshort
            if F_db=="T101a1":
                noVal=xda.Read([ItemContainer(ItemName=os.path.join(p,Tscales_str,"NoVal"))],ReturnItemTime=True)[0][0].Value
                descr=T101a1_descshort
            if F_db=="T101a2":
                noVal=xda.Read([ItemContainer(ItemName=os.path.join(p,Tscales_str,"NoVal"))],ReturnItemTime=True)[0][0].Value
                descr=T101a2_descshort
            if F_db=="Ava":
                noVal=xda.Read([ItemContainer(ItemName=os.path.join(p,Tscales_str,"NoAva"))],ReturnItemTime=True)[0][0].Value
                descr=Ava_descshort
                      
            numbers=min(num,noVal)
            print "Numbers: ",numbers  

            if numbers>0:
                df=catchit(p,Tscales_str,numbers,descr)
                
                try:
                    if not df.empty:
                        df.to_sql(F_db+"_"+Tscales_str, disk_engine, if_exists='append')
                except:
                    pass 
    


  




