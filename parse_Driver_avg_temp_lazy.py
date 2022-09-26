# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 13:22:50 2020

@author: arock
"""
import os

import add_headers_lazy as corpar #not included in package due to sensitive data
import shutil as sh
from zipfile import ZipFile
import sys


stitch_file = 'Stitch_Avg_and_Temp.py'
cwd = os.getcwd().replace("\\","/") + "/"
extractFolder = "Data/Downloads/Temporary/"
readFolder = "Data/Downloads/"
sh.rmtree(cwd+extractFolder,ignore_errors = True)

os.mkdir(cwd+extractFolder)
flag = True 
# if true: add gauges that were installed after initial work was done
# for example: should be false until september 2021 when S1_9a was installed

# months to build out the filenames of all the monthly downloaded zip files
months = ["January","February","March","April","May","June","July","August","September","October","November","December"]

todos = []

# add to do file names from september 2021 to august 2022 
    
for i in range(9,12):
    todos.append(months[i]+"_2021.zip")
    
for i in range(1,8):
    todos.append(months[i]+"_2022.zip")
    
    
# for each monthly zip file
# remove and create a directory to hold the unzipped data files
# extract all data from zip file into temporary folder
# call corrupt line parser to clean each file and put into /New directory for stitching
# call the stitching script to append the new data to the historic data
    
for item in todos:
    sh.rmtree(cwd+extractFolder,ignore_errors = True)
    os.mkdir(cwd+extractFolder)
    readFolder = "Data/Downloads/"
    print(item)
    with ZipFile(cwd+readFolder+item, 'r') as zipObj:
        zipObj.extractall(cwd+extractFolder)
    corpar.cor_Parser() #read in recently downloaded .dat files, remove all corrupted (as well as header) lines, and print csv to /New dir
    sys.argv = [cwd+stitch_file,flag]
    execfile('Stitch_Avg_and_Temp.py') #take the newly parsed/formatted data and stitch it to the historic data


 