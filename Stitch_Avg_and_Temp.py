# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 15:54:35 2019

@author: arock
"""

import pandas as pd
import numpy as np
import datetime as dt
import IDS_update
import time
import sys

start_date = '2019-03-01'
def strip_Choice(Vect,choice):
    """ find the number of values that were either nan or text, so we can subsequently strip away these data"""

    """ strip the zeros (or choice of a specific number) from the dynamicRange structure and put into a DRStrip numpy data structure"""
    num = len(Vect)

    tostrip = 0

    for i in range(num):
        if Vect[i] == float(choice):
            tostrip = tostrip +1
        
    strippedRange = num-tostrip
    
    stripVect= np.zeros((1,strippedRange))
    
    count = 0 
    for j in range(num):
        if Vect[j] == float(choice):
            continue
        else:
            stripVect[0][count] = Vect[j]
            count = count+1
            
            
            
    return stripVect

def Averages_Generator(fileName, col_list_raw, myThresholds,readFolder,saveFolder, init_run=False):

    if col_list_raw == []:
        return [],[]
    # add Max and Min to column name for convenience and reformat into list with max and min for each gauge

    col_list = []
    for col in col_list_raw:
        if "Historic" in readFolder:
            col_list.append("S_"+col+'Avg')
            col_list.append("T_"+col+'Avg')
 
        else:
            col_list.append("S_"+col[:-1]+"_Avg")
            col_list.append("T_"+col[:-1]+"_Avg")
            

        
    #make second list since we changed the reading of the data, but still use original col_list for other logic   
    col_list2 = ["TIMESTAMP"]
    for col in col_list_raw:
        if "Historic" in readFolder:
            col_list2.append("S_"+col+'Avg')
            col_list2.append("T_"+col+'Avg')
 
        else:
            col_list2.append("S_"+col[:-1]+"_Avg")
            col_list2.append("T_"+col[:-1]+"_Avg")

                

    """read comma delimited file, skipping the first junk file and using only the columns we want
    then, filter out all the junk header files mixed in so that we can filter the data by date"""
    rawData = pd.read_csv(fileName, engine='python',skiprows = 0,usecols=col_list2, index_col = False) # we now build data to have headers in first row
    

    
    """ make boolean list of (index,T/F) when certain columns match junk data"""
    timeDrop = (rawData["TIMESTAMP"] == "TIMESTAMP") #or (rawData["TIMESTAMP"] == "TS") or (rawData["TIMESTAMP"] == "NaN")
    tsDrop = (rawData["TIMESTAMP"] == "TS")
    MaxDrop = (rawData[col_list[0]] == "Max")
    TODrop = (rawData["TIMESTAMP"] == "TOA5")
    SMPDrop = (rawData[col_list[0]] == "Smp")
    
    """ put indices into a list; grab the indexes from the weird pandas format and int() them so the indices are readable)"""
    timeIndices = [int(i) for i in rawData.loc[timeDrop].index]
    tsIndices = [int(i) for i in rawData.loc[tsDrop].index]
    MaxIndices = [int(i) for i in rawData.loc[MaxDrop].index]
    TOIndices = [int(i) for i in rawData.loc[TODrop].index]
    SMPIndices = [int(i) for i in rawData.loc[SMPDrop].index]

    """now we drop the rows which have the junk in them"""
    if timeIndices:
        rawData = rawData.drop(timeIndices)
    if tsIndices:
        rawData = rawData.drop(tsIndices)
    if MaxIndices:
        rawData = rawData.drop(MaxIndices)
    if TOIndices:
        rawData = rawData.drop(TOIndices)
    if SMPIndices:
        rawData = rawData.drop(SMPIndices)
    
    """Yikes. We need to reformat all dates into standard format (yyyy-mm-dd HH:MM:SS) because the early data had only one character for month :'( """
    TimeHolder = [dt.datetime.strptime(str(i),'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') for i in rawData["TIMESTAMP"]]
    
    rawData["TIMESTAMP"] = TimeHolder
    
    """cool. now we can make a mask which is another boolean list of (index, T/F) to locate which dates are after start date)"""
    mask = (rawData["TIMESTAMP"]>start_date)

    """make our data of interest (data is our dataFrame) all indices in rawData that match our Mask)"""
    data = rawData.loc[mask]
    
    """start the indices from zero so we can use normal iterables"""
    data.reset_index(drop=True, inplace=True)

    """grab the timestamp column and put it in a dates dataFrame for convenience"""

    


    
    numCol = len(col_list_raw)
    # standard deviation threshold
    
    
    
    #create an 'empty' numpy array data structure to hold the dynamic range of each strain gage

    Results = []


    data_entries = 3 #inlcudes timelist
        
    for i in range(data_entries*(len(col_list_raw))):
        Results.append([])
        

            
    ResultsCount = 0
    
    for i in range(numCol):
        



        
        




#explicitly write the gauges to templist due to substring issue (i.e. G1_1 in both G1_1 and LG1_1)
 
        if col_list_raw[i][0:2] == 'S_':

            tempList = []
            tempList.append("S_"+col_list_raw[i][2:]+'Avg')
            tempList.append("T_"+col_list_raw[i][2:]+'Avg')
            tempList.append("TIMESTAMP")
            tempDF = data[tempList]
            Results[ResultsCount].append(col_list_raw[i][2:])
            Results[ResultsCount+1].append("Temperature_"+col_list_raw[i][2:])
            Results[ResultsCount+2].append("TimeStamps_"+col_list_raw[i][2:])
            if(init_run == False):
                historic_Data = pd.read_csv(saveFolder + "/" + col_list_raw[i][2:] + "Historic.txt", engine='python')
                first_Date = historic_Data["TimeStamps_"+col_list_raw[i][2:]].iloc[-1]
            
        else:

            tempList = []
            tempList.append("S_"+col_list_raw[i]+'Avg')
            tempList.append("T_"+col_list_raw[i]+'Avg')            
            tempList.append("TIMESTAMP") #expect S_gauge and T_gauge plus TIMESTAMP
            tempDF = data[tempList]
            Results[ResultsCount].append(col_list_raw[i])
            Results[ResultsCount+1].append("Temperature_"+col_list_raw[i])
            Results[ResultsCount+2].append("TimeStamps_"+col_list_raw[i])
            if(init_run == False):
                historic_Data = pd.read_csv(saveFolder + "/" + col_list_raw[i] + "Historic.txt", engine='python')
                first_Date = historic_Data["TimeStamps_"+col_list_raw[i]].iloc[-1]
            
        if(init_run == False):
            tempDF = tempDF[tempDF['TIMESTAMP']>first_Date]         
        
        for k in tempList:
            if "T_" in k:
                temperature = tempDF[k].tolist()
            elif "S_" in k:
                strain = tempDF[k].tolist()
            elif "TIMESTAMP" in k:
                timelist = tempDF[k].tolist()



        for j in range(len(strain)):    
            Results[ResultsCount].append(str(strain[j]))
            Results[ResultsCount+1].append(str(temperature[j]))
            Results[ResultsCount+2].append(timelist[j])
            
        ResultsCount += data_entries
        
    return Results
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
if __name__ == '__main__':


    add_S1_9a = False 
    # if true: add gauges that were installed after initial work was done
    # for example: should be false until september 2021 when S1_9a was installed
    
    argvals = sys.argv  #get the arg values if this was executed with arguments passed as a flag
    if len(argvals) >1: #if there is more than zero arguments:
        add_S1_9a = argvals[1]    
    
    
    file_name_S1_9a = 'insert_fname_here' 
    
    suffix = "Historic"
    
    myDict, myThresholds = IDS_update.get_Ids("Latest_Thresholds_6_26_20.csv")# IDS_update would be needed to run, but contains sensitive info
    # initialize list containing the results of D_R.Dynamc_Range. The first entry has names of columns

    saveFolder = "Data/Historic_Avg_Temp"
    
    readFolder = "Data/New"
    
    total_gauges = 111
    if add_S1_9a:
        total_gauges +=1
        myDict[file_name_S1_9a].append("S1_9a_")
    num = 3*total_gauges
    
    #True if its the first run (so there are no historical records to read in in the /data/Historic_Avg_Temps/ folder)
    init_run = False ############
#    saveFolder = "Data/New"
    vals = [] 
    print("Beginning the Data Generation Process!\n")
    start = time.clock()
    
    for i in range(num):
        vals.append([])

    countVal = 0
    for key in myDict.keys(): 
        holder = Averages_Generator(readFolder + '/' + key, myDict[key],myThresholds,readFolder,saveFolder,init_run)  
        for i in range(len(holder)):
            for j in range(len(holder[i])):  
                vals[countVal].append(holder[i][j])
            countVal += 1
        print("The loops are {}% done!".format(np.round((countVal/float(num))*100,1)))
                
    print("Now on to printing the CSV! :D")            
                

    # generic file for output file with mathcad stripped data
    for k in range(num/3): #3 streams of data per file
        gauge = vals[3*k][0]
        outFile = saveFolder+ '/'+ gauge + suffix + ".txt"
        #
        ##write values of interest to file
        if init_run == True:
            startnum = 0
        else:
            startnum = 1
        fn = open(outFile,"a")        
        for j in range(startnum,len(vals[3*k])): #only put the data in , not the header if not first run
            for i in range(3):
                if i<2:
                    fn.write(str(vals[3*k+i][j])+",")
                else:
                    fn.write(str(vals[3*k+i][j])+"\n")
        fn.close()   
    
    
    
    end = time.clock()
    
    print("\nThe Data Generation process took {} seconds to run.".format(end-start))
    