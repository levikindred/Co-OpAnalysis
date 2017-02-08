# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 17:07:59 2016

@author: Levi
"""

import sqlite3
import os

inputFP = "C:\\Users\\Levi\\Desktop\\applied data analytics\\Wedge\\split data\\"
inputFPs = os.listdir(path = inputFP)

byHour = {}
byOwner = {}
byProduct = {}

db = sqlite3.connect("C:\\Users\\Levi\\Desktop\\applied data analytics\\Wedge\\wedge.db")
cur = db.cursor()
cur.execute('''DROP TABLE IF EXISTS by_owner''')
cur.execute('''CREATE TABLE by_owner (
    card_no INTEGER,
    year INTEGER,
    month INTEGER,
    sales REAL,
    transactions INTEGER,
    items INTEGER)''')

for i, fp in enumerate(inputFPs):
    with open(inputFP + fp, "r") as inputF:
        inputF.readline()
        
        for j, line in enumerate(inputF):
            values = line.strip().split(",")
            
            if (values[9] != "0" and values[9] != "15"):
                if (values[8] == " " or values[8] == ""):
                    datehour = values[0][0:-6]
                    if(datehour in byHour):
                        byHour[datehour][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byHour[datehour][1]:
                            byHour[datehour][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byHour[datehour][2] += 1
                    else:
                        byHour[datehour] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], 1, 0]
                    
                    year = values[0][0:4]
                    month = values[0][5:7]

                    try:
                        owner = values[45]
                    except IndexError:
                        print("something wrong with " + str(j))
                        continue

                    if((year, month, owner) in byOwner):
                        byOwner[(year, month, owner)][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byOwner[(year, month, owner)][1]:
                            byOwner[(year, month, owner)][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byOwner[(year, month, owner)][2] += 1
                    else:
                        byOwner[(year, month, owner)] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], 1]
                    
                    upc = values[4]

                    if((year, month, upc) in byProduct):
                        byProduct[(year, month, upc)][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byProduct[(year, month, upc)][1]:
                            byProduct[(year, month, upc)][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byProduct[(year, month, upc)][2] += 1
                    else:
                        byProduct[(year, month, upc)] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], 1, values[5], values[9], 0]
                    
                if (values[8] == "V" or values[9] == "R"):
                    datehour = values[0][0:-6]
                    if(datehour in byHour):
                        byHour[datehour][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byHour[datehour][1]:
                            byHour[datehour][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byHour[datehour][2] -= 1
                    else:
                        byHour[datehour] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], -1, 0]
                    
                    year = values[0][0:4]
                    month = values[0][5:7]

                    try:
                        owner = values[45]
                    except IndexError:
                        print("something wrong with " + str(j))
                        continue

                    if((year, month, owner) in byOwner):
                        byOwner[(year, month, owner)][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byOwner[(year, month, owner)][1]:
                            byOwner[(year, month, owner)][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byOwner[(year, month, owner)][2] -= 1
                    else:
                        byOwner[(year, month, owner)] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], -1]
                    
                    upc = values[4]

                    if((year, month, upc) in byProduct):
                        byProduct[(year, month, upc)][0] += float(values[12])
                        if (values[0][:-9], values[1], values[2], values[3]) not in byProduct[(year, month, upc)][1]:
                            byProduct[(year, month, upc)][1].append((values[0][:-9], values[1], values[2], values[3]))
                        byProduct[(year, month, upc)][2] -= 1
                    else:
                        byProduct[(year, month, upc)] = [float(values[12]), [(values[0][:-9], values[1], values[2], values[3])], -1, values[5], values[9], 0]
                
                if j%100000 == 0:
                    print("processed " + str(j) + " lines of file " + str(i))
                    
    for key in byOwner:
        cur.execute('''INSERT INTO by_owner (card_no, year, month, sales, transactions, items)
        VALUES(?,?,?,?,?,?)''', [key[2], key[0], key[1], byOwner[key][0], len(set(byOwner[key][1])), byOwner[key][2]])
    byOwner = {}
    
    for key in byHour:
        byHour[key][3] += len(set(byHour[key][1]))
        byHour[key][1] = []
        
    for key in byProduct:
        byProduct[key][5] += len(set(byProduct[key][1]))
        byProduct[key][1] = []


depts = ["PACKAGED GROCERY","PRODUCE","BULK","REF GROCERY","CHEESE","FROZEN","BREAD","DELI","GEN MERCH","SUPPLEMENTS","PERSONAL CARE","HERBS&SPICES","MEAT","JUICE BAR","MISC P/I","FISH&SEAFOOD","BAKEHOUSE","FLOWERS","WEDGEWORLDWIDE","MISC P/I - WWW","CATERING","BEER & WINE"]


cur.execute('''DROP TABLE IF EXISTS by_hour''')
cur.execute('''CREATE TABLE by_hour (
    date DATE,
    hour INTEGER,
    sales REAL,
    transactions INTEGER,
    items INTEGER)''')
    
for key in byHour:
    cur.execute('''INSERT INTO by_hour (date, hour, sales, transactions, items)
    VALUES(?,?,?,?,?)''', [key[0:-3], key[-2:], byHour[key][0], len(set(byHour[key][1])) + byHour[key][3], byHour[key][2]])

    
cur.execute('''DROP TABLE IF EXISTS by_product''')
cur.execute('''CREATE TABLE by_product (
    upc INTEGER,
    description TEXT,
    dept_no INTEGER,
    dept TEXT,
    year INTEGER,
    month INTEGER,
    sales REAL,
    transactions INTEGER,
    items INTEGER)''')

for key in byProduct:
    cur.execute('''INSERT INTO by_product (upc, description, dept_no, dept, year, month, sales, transactions, items)
    VALUES(?,?,?,?,?,?,?,?,?)''', [key[2], byProduct[key][3], byProduct[key][4], depts[int(byProduct[key][4]) - 1], key[0], key[1], byProduct[key][0], len(set(byProduct[key][1])) + byProduct[key][5], byProduct[key][2]])

db.commit()
db.close()