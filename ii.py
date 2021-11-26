
import urllib.request
fileobject = urllib.request.urlopen("http://www.sidc.be/silso/DATA/SN_d_tot_V2.0.txt")

import pandas as pd
import numpy as np
df = pd.DataFrame(columns = ["Date", "Sunspot_count", "Sunspot_sd", "Observ_No"])
import sqlite3
conn = sqlite3.connect("space.db", isolation_level=None)
cur = conn.cursor()
cur.execute('''
    CREATE TABLE sunspots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    sunspot_count INTEGER,
    sunspot_sd REAL,
    sunspot_obs_no INTEGER
    );
    ''')

for line in fileobject:
    row_bytes = line.split()

    date = row_bytes[0].decode("utf-8") + "-" + row_bytes[1].decode("utf-8") + "-" + row_bytes[2].decode("utf-8")    
    row_txt = [date, row_bytes[4].decode("utf-8"), row_bytes[5].decode("utf-8"), row_bytes[6].decode("utf-8")] 
    a_series = pd.Series(row_txt, index = df.columns)
    
    query = 'INSERT INTO sunspots (date, sunspot_count, sunspot_sd, sunspot_obs_no) VALUES ("%s", "%s", "%s", "%s")' % (a_series["Date"], a_series["Sunspot_count"], a_series["Sunspot_sd"], a_series["Observ_No"])
    cur.execute(query)

import json
import urllib 
url_mag="https://services.swpc.noaa.gov/products/solar-wind/mag-7-day.json"
url_plasma="https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json"
mag=urllib.request.urlopen(url_mag)
plasma=urllib.request.urlopen(url_plasma)
mag_json=json.loads(mag.read())
plasma_json=json.loads(plasma.read())



import sqlite3
conn = sqlite3.connect("space.db", isolation_level=None)
cur = conn.cursor()
cur.execute('''
    CREATE TABLE mag (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_time DATETIME,
    bx REAL,
    by REAL,
    bz REAL,
    bt REAL
    );
    ''')
cur.execute('''
    CREATE TABLE plasma (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_time DATETIME,
    density REAL,
    speed REAL,
    temp REAL
    );
    ''')
    
    
query = 'INSERT INTO mag (date_time, bx, by, bz, bt) VALUES ("%s", "%s", "%s", "%s", "%s")' % (line[0][:19], line[1], line[2], line[3], line[6])    
cur.execute(query)
conn.commit()

import ftplib#Open ftp connection
ftp = ftplib.FTP('ftp.seismo.nrcan.gc.ca', 'anonymous','user')#List the files in the current directory
print("File List:")
files = ftp.dir()
import datetime
now=datetime.datetime.now()
ftp.cwd("intermagnet/minute/provisional/IAGA2002/" + str(now.year) + "/" + str(now.strftime("%m")))
# files = ftp.dir()

file_list = ftp.nlst()
import pandas as pd
import numpy as np
df = pd.DataFrame(columns = ["Date_time", "Bx", "By", "Bz", "Bf"])
import sqlite3
conn = sqlite3.connect("space.db", isolation_level=None)
cur = conn.cursor()
cur.execute('''
    CREATE TABLE geo_mag (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station CHARACTER(3),
    lat REAL,
    long REAL,
    date_time DATETIME,
    bx REAL,
    by REAL,
    bz REAL,
    bf REAL
    );
    ''')


for file in file_list:
    station = ''
    lat = 0
    long = 0
    
    date_today = str(now.year) + str(now.strftime("%m")) + str(now.strftime("%d"))
    
    if(date_today in file):
        ftp.retrbinary("RETR " + file, open(file, 'wb').write)
        temp=open(file, 'rb')
        
        data_rows = 0 
        for line in temp:    
            if(data_rows == 1):
                row_bytes = line.split()
                date_time = row_bytes[0].decode("utf-8") + " " + row_bytes[1].decode("utf-8")[:8]
                row_txt = [date_time, row_bytes[3].decode("utf-8"), row_bytes[4].decode("utf-8"), row_bytes[5].decode("utf-8"), row_bytes[6].decode("utf-8")]
                
                a_series = pd.Series(row_txt, index = df.columns)
    
                query = 'INSERT INTO geo_mag (station, lat, long, date_time, bx, by, bz, bf) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % (station, lat, long, a_series["Date_time"], a_series["Bx"], a_series["By"], a_series["Bz"], a_series["Bf"]) 
                cur.execute(query)
            else:
                if('IAGA Code' in line.decode("utf-8") or 'IAGA CODE' in line.decode("utf-8")):
                    station = line.decode('utf-8').split()[2]
                    print(station)
                elif('Latitude' in line.decode("utf-8")):
                    lat = line.decode('utf-8').split()[2]
                elif('Longitude' in line.decode("utf-8")):
                    long = line.decode('utf-8').split()[2]
                elif('DATE       TIME' in line.decode("utf-8")):
                    data_rows = 1        
        conn.commit()
