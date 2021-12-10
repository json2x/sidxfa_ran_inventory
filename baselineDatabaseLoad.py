#!/usr/bin/env /home/jcm/env/sidxfa/bin/python

import os, glob, sys, re, time
from shutil import copyfile
import pandas as pd
import numpy as np
from mydataframe import dataframe
from database import database
from datetime import datetime, timedelta
import openpyxl

SOURCE_FILE_DIR = '/home/jcm/mnt/OVIM_SITEDB/TCM_RAN_SID'
DEVICE_FILENAME = 'Device List Report - ALL *'
CELL_FILENAME = 'List Report - CELL *'

#------------------------------
def main():
	'''
	Prep, Set referential integrity of device and cell list tables
	'''
	##### DEVICE #####
	device_file = getLatestFile(SOURCE_FILE_DIR + '/' + DEVICE_FILENAME)
	'''
	1.) Get file into pandas data frame
	'''
	print("Putting data in [{}] into a dataframe...".format(device_file),end="", flush=True)
	deviceDf = dataframe(device_file)
	deviceDf.setColsToRename({'FREQ (TX/RX)':'FREQ (TX_RX)'})
	deviceDf.createExcelDataframe(sheetname='Sheet1', skiprows=2)
	deviceDf.df['record_status'] = 0
	print("[OK]\n")
	
	'''
	2.) Iterate thru devices and add them one by one in db table
	'''
	db = database('10.150.20.102', 'ovimadmin', '0vimadmin!', 'smart_site')
	if db.connect():	
		failed_count = 0
		for i,row in deviceDf.df.iterrows():
			try:
				row = row.fillna('')
				row_dict = row.to_dict()
				db.table('devices')
				db.insert(row_dict)
				if not db.notification['status']:
					print("Failed to insert: {}".format(row['Device ID']))
					print(db.notification['text'])
					print("")
					failed_count = failed_count + 1
					
			except Exception as e:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
				print("Failed to insert: {}".format(row['Device ID']))
				print("{}: {}\n{}\n".format(exc_type, exc_obj, fname))
				failed_count = failed_count + 1
		db.disconnect()
		print("{}/{} rows failed to load\n".format(failed_count, i))
	else:
		print('Database connection failed.')
	
	##### CELLS #####
	cell_file = getLatestFile(SOURCE_FILE_DIR + '/' + CELL_FILENAME)
	'''
	1.) Get file into pandas data frame
	'''
	print("Putting data in [{}] into a dataframe...".format(cell_file),end="", flush=True)
	cellsDf = dataframe(cell_file)
	cellsDf.createExcelDataframe(sheetname='Sheet1', skiprows=2)
	cellsDf.df['record_status'] = 0
	print("[OK]\n")
	
	'''
	2.) Iterate thru cells and add them one by one in db table, drop cells with device reference error
	'''
	if db.connect():
		failed_count = 0
		for i,row in cellsDf.df.iterrows():
			try:
				row = row.fillna('')
				row_dict = row.to_dict()
				db.table('cells')
				db.insert(row_dict)
				if not db.notification['status']:
					print("Failed to insert: {} -> {}".format(row['Parent ID'], row['Cell Name']))
					print(db.notification['text'])
					print("")
					failed_count = failed_count + 1
			except Exception as e:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
				print("Failed to insert: {} -> {}".format(row['Parent ID'], row['Cell Name']))
				print("{}: {}\n{}\n".format(exc_type, exc_obj, fname))
				failed_count = failed_count + 1
		db.disconnect()
		print("{}/{} rows failed to load\n".format(failed_count, i))
	else:
		print('Database connection failed.')
	
	
#---------------------------
def getLatestFile(filePath):
	list_of_files = glob.glob(filePath)
	latest_file = max(list_of_files, key=os.path.getctime)
	
	return latest_file
	
if __name__ == "__main__":
	starTime = datetime.now()
	print("---------------------")
	print("Baseline database load.")
	print("---------------------\n")
	
	main()
	
	endTime = datetime.now()
	deltaDt = endTime - starTime
	print(deltaDt)
	print("<<<<< End of Script >>>>>")
