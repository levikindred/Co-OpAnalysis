# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 17:06:57 2016

@author: Levi
"""

import os
import zipfile

headers = 'datetime,register_no,emp_no,trans_no,upc,description,trans_type,trans_subtype,trans_status,department,quantity,Scale,cost,unitPrice,total,regPrice,altPrice,tax,taxexempt,foodstamp,wicable,discount,memDiscount,discountable,discounttype,voided,percentDiscount,ItemQtty,volDiscType,volume,VolSpecial,mixMatch,matched,memType,staff,numflag,itemstatus,tenderstatus,charflag,varflag,batchHeaderID,local,organic,display,receipt,card_no,store,branch,match_id,trans_id\n'

inputFP = "C:\\Users\\Levi\\Desktop\\applied data analytics\\Wedge\\data"
inputFPs = os.listdir(path = inputFP)

outputFP = "C:\\Users\\Levi\\Desktop\\applied data analytics\\Wedge\\split data"
outputFiles = []
for i in range(199):
    outputFiles.append(open(outputFP + "\\MemberDataFile" + str(i) + ".csv", "w"))
    outputFiles[i].write(headers)

numNonMemF = 0
numNonMemFLines = 0
nonMemF = open(outputFP + "\\NonMemberDataFile" + str(numNonMemF) + ".csv", "w")
nonMemF.write(headers)

for i, fp in enumerate(inputFPs):
    delimiter = b","
    
    with zipfile.ZipFile(inputFP + "//" + fp) as zipF:
        with zipF.open(fp[0:-4] + ".csv") as inputF:
            firstLine = inputF.readline().replace(b"\"", b"")
            
            if firstLine.count(b";") > 10:
                delimiter = b";"
            
            if firstLine[0:8] != b'datetime':
                values = firstLine.split(delimiter)
                memNum = values[45].decode('utf-8')
                
                if memNum != '3' and memNum != '' and memNum != ' ':
                    outF = outputFiles[int(memNum[1:-1])%199]
                else:
                    if(numNonMemFLines == 400000):
                        numNonMemF += 1
                        numNonMemFLines = 0
                        nonMemF.close()
                        nonMemF = open(outputFP + "\\NonMemberDataFile" + str(numNonMemF) + ".csv", "w")
                        nonMemF.write(headers)
                    outF = nonMemF
                    numNonMemFLines += 1
                
                outF.write(values[0].decode('utf-8'))
                for value in values[1:]:
                    outF.write(",")
                    outF.write(value.decode('utf-8'))
                outF.write("\n")
            
            for j, line in enumerate(inputF):
                if(j % 100000 == 0): print("Processed the " + str(j) + "th line of " + str(i) + "th file")                
                
                values = line.strip().replace(b"\"", b"").split(delimiter)
                memNum = values[45].decode('utf-8')
                
                if memNum != '3' and memNum != '' and memNum != ' ':
                    outF = outputFiles[int(memNum[1:-1])%199]
                else:
                    if(numNonMemFLines == 400000):
                        numNonMemF += 1
                        numNonMemFLines = 0
                        nonMemF.close()
                        nonMemF = open(outputFP + "\\NonMemberDataFile" + str(numNonMemF) + ".csv", "w")
                        nonMemF.write(headers)
                    outF = nonMemF
                    numNonMemFLines += 1
                
                outF.write(values[0].decode('utf-8'))
                for value in values[1:]:
                    outF.write(",")
                    outF.write(value.decode('utf-8'))
                outF.write("\n")

nonMemF.close()
for i in range(198):
    outputFiles[i].close()