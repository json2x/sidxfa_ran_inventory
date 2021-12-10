#!/usr/bin/env /home/jcm/env/sidxfa/bin/python

import os, glob, sys, re, time
from shutil import copyfile
import pandas as pd
import numpy as np
from mydataframe import dataframe
from database import database
from datetime import datetime, timedelta
import openpyxl

DIR_SID = '/home/jcm/mnt/SID/'
DIR_RAN = '/home/jcm/mnt/RAN/'
DIR_LOCAL = '/home/jcm/projects/SIDxFARanInventory/LocalFiles/'
OUTPUT_DIR = '/home/jcm/mnt/OVIM_SITEDB/'

SID_RAN_FILE = {
	'Device': '/home/jcm/mnt/SID/Device*',
	'Cell': '/home/jcm/mnt/SID/*CELL*'
}

NMS_RAN_FILE = {
	'Device': '/home/jcm/mnt/RAN/RAN_DEVICE*',
	'Cell': '/home/jcm/mnt/RAN/RAN_CELL*'
}

LOCAL_RAN_FILE = {
	'SID Device': '',
	'SID Cell': '',
	'NMS Device': '',
	'NMS Cell': ''
}

timestr = time.strftime('%Y%m%d')
NOCFA_SMART_NEs = dataframe()
SID_NEs = dataframe()

#---------------------------
def main():
	#print("Run date {}\n".format(time.strftime('%Y-%m-%d %H:%M')))
	#Get latest RAN files and copy to local directory
	getLatestFileAndStoreLocally()
	
	#Get SID NEs and store to df
	SID_NEs.df = getSIDCellNEs()
	
	#Connect to FA database and store NEs to df
	NOCFA_SMART_NEs = getNOCFASmartNEs()
	
	'''
	CHECK IF THIS IS NECESSARY TO STORE
	'''
	#print("\nGet records in SID and not in NOC-FA...")
	#inSIDNotInNOCFA = getRecordsInSIDNotInNOCFA(SID_NEs.df, NOCFA_SMART_NEs.df)
	
	print("\nGet records in NOC-FA and not in SID...")
	inNOCFANotInSID = getRecordsInNOCFANotInSID(SID_NEs.df, NOCFA_SMART_NEs.df)
	
	print("\nGet RAN CELL data for records in NOC-FA and not in SID...")
	inNOCFARANCELLData = getInNOCFANotInSIDToRANCELL(inNOCFANotInSID)
	#For memory cleanup
	inNOCFANotInSID = None
	
	print("\nGet match records from SID and NOC-FA...")
	matchSIDvsNOCFA = getMatchedRecordsFromSIDAndNOCFA(SID_NEs.df, NOCFA_SMART_NEs.df)
	
	print("\nSaving final cell list file...")
	cellListDf = saveFinalCellList(matchSIDvsNOCFA, inNOCFARANCELLData)
	#For memory cleanup
	#cleanup memory space
	NOCFA_SMART_NEs = None
	SID_NEs.df = None
	matchSIDvsNOCFA = None
	inNOCFARANCELLData = None
	
	varianceDevicesDf = getDeviceVarianceOfCellListAndSID(cellListDf)
	
	devicesOfVarianceFromRANCell = getDeviceOfVarianceFromRANDeviceList(varianceDevicesDf)
	#cleanup memory space
	varianceDevicesDf = None
	
	matchDevicesDf = getDeviceOfCellListFromSID(cellListDf)
	
	theRestOfdeviceListDf = getNonMobileRANDevices()
	
	saveFinalDeviceList(theRestOfdeviceListDf, matchDevicesDf, devicesOfVarianceFromRANCell)
	#cleanup memory space
	theRestOfdeviceListDf = None
	matchDevicesDf = None
	devicesOfVarianceFromRANCell = None
	
	
	
	#concat deviceListDf, matchDevicesDf, devicesOfVarianceFromRANCell
	
	
#---------------------------
def getLatestFileAndStoreLocally():
	#Get latest SID RAN files and copy to local directory
	for Name in SID_RAN_FILE:
		print("Latest {} list file.".format(Name))
		latest_file = getLatestFile(SID_RAN_FILE[Name])
		print(latest_file)
		
		print("Moving file to local directory for processing...")
		localFile = copyFileToLocalDir(latest_file, DIR_SID)
		LOCAL_RAN_FILE["SID {}".format(Name)] = localFile
		print(localFile)
		print("")
		
	#Get latest NMS RAN files and copy to local directory
	for Name in NMS_RAN_FILE:
		print("Latest {} list file.".format(Name))
		latest_file = getLatestFile(NMS_RAN_FILE[Name])
		print(latest_file)
		
		print("Moving file to local directory for processing...")
		localFile = copyFileToLocalDir(latest_file, DIR_RAN)
		LOCAL_RAN_FILE["NMS {}".format(Name)] = localFile
		print(localFile)
		print("")
	
		
#---------------------------
def getLatestFile(filePath):
	list_of_files = glob.glob(filePath)
	latest_file = max(list_of_files, key=os.path.getctime)
	
	return latest_file
	
#---------------------------
def copyFileToLocalDir(filePath, originatingDir):
	localFilePath = DIR_LOCAL + filePath.replace(originatingDir, '')
	copyfile(filePath, localFilePath)
	
	return localFilePath

#---------------------------	
def getSIDCellNEs():
	print("Get SID NEs to Pandas dataframe...")
	sidran = pd.ExcelFile(LOCAL_RAN_FILE['SID Cell']).parse('Sheet1', skiprows=2)
	sidran['Band'] = sidran['Band'].str.replace('DCS','')
	sidran['Band'] = sidran['Band'].str.replace('GSM','')
	sidran['Band'] = sidran['Band'].str.replace('gsm','')
	sidran['Site'] = sidran['Site'].astype(str)
	sidran['Band'] = sidran['Band'].astype(str)
	
	sidran.loc[sidran['Tech'] == 'CELL_TDD', 'SUBDOMAIN'] = 'TD-LTE'
	sidran.loc[sidran['Tech'] == 'CELL_FDD', 'SUBDOMAIN'] = 'FD-LTE'
	sidran.loc[sidran['Tech'] == 'FDD-LTE', 'SUBDOMAIN'] = 'FD-LTE'
	sidran.loc[sidran['Tech'] == 'FDD', 'SUBDOMAIN'] = 'FD-LTE'
	sidran.loc[sidran['Tech'] == 'TDD', 'SUBDOMAIN'] = 'TD-LTE'
	sidran.loc[sidran['Tech'] == '3G', 'SUBDOMAIN'] = '3G'
	sidran.loc[sidran['Tech'] == '2G', 'SUBDOMAIN'] = '2G'
	sidran.loc[sidran['Tech'].isnull(), 'SUBDOMAIN'] = 'X'
	sidran['SUBDOMAIN'] = sidran['SUBDOMAIN'].astype(str)
	
	print("SID NEs successfully stored in dataframe.")
	return sidran
	
#---------------------------
def getNOCFASmartNEs():
	#Connect to FA database
	print("\nConnecting to NOC-FA Site Database...", end="")
	siteDB = database('10.150.20.101', 'ovim', 'ovim', 'smartSiteDB')
	smartne = dataframe()
	if siteDB.connect():
		print("[OK]")
		sql = "select vendor as Vendor, bsc as Homing, siteid as Site, band as Band, case when id then '2G' end as SUBDOMAIN from 2g_ne UNION \
		select vendor as Vendor, rnc as Homing, siteid as Site, band as Band, case when id then '3G' end as SUBDOMAIN from 3g_ne UNION \
		select vendor as Vendor, '' as Homing, siteid as Site, band as Band, case when id then 'FD-LTE' end as SUBDOMAIN from fdlte_ne UNION \
		select vendor as Vendor, '' as Homing, siteid as Site, band as Band, case when id then 'TD-LTE' end as SUBDOMAIN from tdlte_ne UNION \
		select vendor as Vendor, '' as Homing, siteid as Site, band as Band, case when id then '5G' end as SUBDOMAIN from 5g_ne"
		print("Get NOC-FA NEs to Pandas dataframe...")
		smartne.createSQLResultDataframe(sql, siteDB.conn)
		smartne.df['Site'] = smartne.df['Site'].astype(str)
		smartne.df['Band'] = smartne.df['Band'].astype(str)
		smartne.df['SUBDOMAIN'] = smartne.df['SUBDOMAIN'].astype(str)
		print("Smart NEs successfully stored in dataframe.")
	else:
		print("[FAILED]")
		
	return smartne
	
#---------------------------
def getMatchedRecordsFromSIDAndNOCFA(siddf, nocfadf):
	try:
		matchSIDvsNOCFA = pd.merge(siddf, nocfadf, on=['Site', 'Band', 'SUBDOMAIN'], how='inner')
		matchSIDvsNOCFA = matchSIDvsNOCFA.drop(['Homing', 'Vendor'], axis=1)
		return matchSIDvsNOCFA
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("Unexpected error in getting match records.")
		print(exc_type, fname, exc_tb.tb_lineno)
	
#---------------------------
def getRecordsInSIDNotInNOCFA(siddf, nocfadf):
	try:
		inSIDNotInNOCFA = pd.merge(siddf, nocfadf, on=['Site', 'Band', 'SUBDOMAIN'], how='left')
		inSIDNotInNOCFA = inSIDNotInNOCFA[inSIDNotInNOCFA['Homing'].isnull()]
		inSIDNotInNOCFA = inSIDNotInNOCFA.drop(['Homing', 'Vendor'], axis=1)
		print("Variance (In SID not in NOCFA) rows dataframe size: {}".format(len(inSIDNotInNOCFA.index)))
		saveDataframeToFile(inSIDNotInNOCFA, 'FoundInSIDNotInNOCFA_{}'.format(timestr))
		
		return inSIDNotInNOCFA
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("Unexpected error in getting records in SID and not in NOC-FA.")
		print(exc_type, fname, exc_tb.tb_lineno)
	
#---------------------------
def getRecordsInNOCFANotInSID(siddf, nocfadf):
	try:
		inNOCFANotInSID = pd.merge(nocfadf, siddf, on=['Site', 'Band', 'SUBDOMAIN'], how='left')
		inNOCFANotInSID = inNOCFANotInSID[inNOCFANotInSID['Domain'].isnull()]
		inNOCFANotInSID = inNOCFANotInSID[['Vendor', 'Homing', 'Site', 'Band', 'SUBDOMAIN']]
		print("Variance (In NOCFA not in SID) rows dataframe size: {}".format(len(inNOCFANotInSID.index)))
		#saveDataframeToFile(inNOCFANotInSID, 'FoundInNOCFANotInSID_{}'.format(timestr))
		
		return inNOCFANotInSID
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("Unexpected error in getting records in NOC-FA and not in SID.")
		print(exc_type, fname, exc_tb.tb_lineno)

#---------------------------
def getInNOCFANotInSIDToRANCELL(nocfadf):
	try:
		colsToRename = {
			'EMS_CELL_ID': 'EMS Cell ID', 'EMS_ID': 'EMS ID', 'CELL_NAME': 'Cell Name', 
			'SITE_ID': 'Site', 'PARENT_ID': 'Parent ID', 'PARENT_DN': 'Parent DN', 'TECH': 'Tech', 
			'BAND': 'Band', 'ADMIN_STATE': 'Admin State', 'ALIAS': 'Alias', 'LAC_TAC': 'LAC TAC', 
			'SAC_CI_EUTRA': 'SAC CI EUTRA', 'RNC_CID': 'RNC CID', 'PHY_CID': 'PHY CID', 'LCR_CID': 'LCR CID', 
			'SECTOR_ID': 'SECTOR ID', 'NE_TYPE': 'NE TYPE', 'SDCCH_CAP': 'SDCCH CAP', 'TCH_CAP': 'TCH CAP'
		}
		
		nmsrancell = dataframe(LOCAL_RAN_FILE['NMS Cell'])
		nmsrancell.setColsToRename(colsToRename)
		nmsrancell.createCSVDataframe()
		
		nmsrancell.df['Band'] = nmsrancell.df['Band'].str.replace('DCS','')
		nmsrancell.df['Band'] = nmsrancell.df['Band'].str.replace('GSM','')
		nmsrancell.df['Site'] = nmsrancell.df['Site'].astype(str)
		nmsrancell.df['Band'] = nmsrancell.df['Band'].astype(str)
		
		nmsrancell.df.loc[nmsrancell.df['Tech'] == 'CELL_TDD', 'SUBDOMAIN'] = 'TD-LTE'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == 'CELL_FDD', 'SUBDOMAIN'] = 'FD-LTE'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == 'FDD-LTE', 'SUBDOMAIN'] = 'FD-LTE'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == 'FDD', 'SUBDOMAIN'] = 'FD-LTE'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == 'TDD', 'SUBDOMAIN'] = 'TD-LTE'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == '3G', 'SUBDOMAIN'] = '3G'
		nmsrancell.df.loc[nmsrancell.df['Tech'] == '2G', 'SUBDOMAIN'] = '2G'
		nmsrancell.df.loc[nmsrancell.df['Tech'].isnull(), 'SUBDOMAIN'] = 'X'
		nmsrancell.df['SUBDOMAIN'] = nmsrancell.df['SUBDOMAIN'].astype(str)
		
		matchNOCFAAndNMS = pd.merge(nmsrancell.df, nocfadf, on=['Site', 'Band', 'SUBDOMAIN'], how='inner')
		matchNOCFAAndNMS = matchNOCFAAndNMS.drop(['Homing', 'Vendor', 'HOMING_ID', 'DLEARFCN', 'ULEARFCN', 'DLCHBW', 'ULCHBW', 
		'RAC', 'NCC', 'BCC', 'NNODEID', 'NBSCID', 'PSC', 'BCCHNO'], axis=1)
		matchNOCFAAndNMS.insert(0, 'Domain', 'RAN')
		matchNOCFAAndNMS['Azimuth'] = ''
		
		inNOCFANotInNMS = pd.merge(nocfadf, nmsrancell.df, on=['Site', 'Band', 'SUBDOMAIN'], how='left')
		inNOCFANotInNMS = inNOCFANotInNMS[inNOCFANotInNMS['EMS ID'].isnull()]
		
		#Maybe write the remaining variance to a file. for data mgmt validation ang mga natirang variance
		saveDataframeToFile(inNOCFANotInNMS, 'NESInNOCFANotInNMS_{}'.format(timestr))
		print("Record count of variance: {}".format(len(inNOCFANotInNMS.index)))
		
		return matchNOCFAAndNMS
	
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("Unexpected error in getting RAN CELL data of records in NOC-FA and not in SID.")
		print(exc_type, fname, exc_tb.tb_lineno)
	
#---------------------------
def saveFinalCellList(df1, df2):
	try:
		cellListDf = pd.concat([df1,df2],ignore_index=True).reset_index(drop=True)
		df1 = None
		df2 = None
		isSaved = saveDataframeToFile(cellListDf, "TCM_RAN_SID/List Report - CELL {}".format(timestr), 2)
		xl_file = OUTPUT_DIR + "TCM_RAN_SID/List Report - CELL {}.xlsx".format(timestr)
		if isSaved:
			#### edit here ###
			xfile = openpyxl.load_workbook(xl_file)

			sheet = xfile.get_sheet_by_name('Sheet1')
			sheet['A1'] = 'List Report - Cell'
			sheet['A2'] = ' '
			xfile.save(xl_file)
		elif os.path.exists(xl_file):
			#delete the file
			os.remove(xl_file)
		
	except:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		xl_file = OUTPUT_DIR + "TCM_RAN_SID/List Report - CELL {}.xlsx".format(timestr)
		print("Unexpected error in saving final cell list file.")
		print(exc_type, fname, exc_tb.tb_lineno)
		if os.path.exists(xl_file):
			#delete the file
			os.remove(xl_file)
		
	return cellListDf
	
#---------------------------
def saveFinalDeviceList(df1, df2, df3):
	try:
		deviceListDf = pd.concat([df1,df2,df3],ignore_index=True).reset_index(drop=True)
		df1 = None
		df2 = None
		df3 = None
		isSaved = saveDataframeToFile(deviceListDf, "TCM_RAN_SID/Device List Report - ALL {}".format(timestr), 2)
		xl_file = OUTPUT_DIR + "TCM_RAN_SID/Device List Report - ALL {}.xlsx".format(timestr)
		
		if isSaved:
			#### edit here ###
			xfile = openpyxl.load_workbook(xl_file)

			sheet = xfile.get_sheet_by_name('Sheet1')
			sheet['A1'] = 'Device List Report - All'
			sheet['A2'] = ' '
			xfile.save(xl_file)
		elif os.path.exists(xl_file):
			#delete the file
			os.remove(xl_file)
	except:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		xl_file = OUTPUT_DIR + "TCM_RAN_SID/List Report - CELL {}.xlsx".format(timestr)
		print("Unexpected error in saving final device list file.")
		print(exc_type, fname, exc_tb.tb_lineno)
		if os.path.exists(xl_file):
			#delete the file
			os.remove(xl_file)
	
#---------------------------
def getDeviceOfCellListFromSID(cellDeviceListDf):
	print("Get SID RAN Devices to Pandas dataframe...")
	cellDeviceListDf = cellDeviceListDf[['Parent ID', 'Parent DN']]
	siddevices = pd.ExcelFile(LOCAL_RAN_FILE['SID Device']).parse('Sheet1', skiprows=2)
	siddevices = siddevices.sort_values(by=['Domain']).reindex()
	cellDeviceListDf = cellDeviceListDf.rename(columns={'Parent ID': 'Device ID'})
	
	sidrandevices = siddevices.loc[((siddevices['NE Type'] == 'BT') | (siddevices['NE Type'] == 'NB') | (siddevices['NE Type'] == 'LT') | (siddevices['NE Type'] == 'TD'))]
	
	print("SID RAN devices count: {}".format(len(sidrandevices.index)))
	matchCellAndDevice = pd.merge(sidrandevices, cellDeviceListDf, on=['Device ID'], how='inner')
	matchCellAndDevice = matchCellAndDevice.drop(['Parent DN'], axis=1)
	
	matchCellAndDevice = matchCellAndDevice.drop_duplicates(subset=['DN', 'Device ID', 'EMS ID', 'NE Type', 'Parent Device ID'])
	matchCellAndDevice = getNEOwnerOfDevicesFromNOCFADB(matchCellAndDevice)
	print("Match count: {}".format(len(matchCellAndDevice.index)))
	
	return matchCellAndDevice
	
#---------------------------
def getDeviceVarianceOfCellListAndSID(cellDeviceListDf):
	print("Get Variance of SID RAN Devices and Cell list devices to Pandas dataframe...")
	siddevices = pd.ExcelFile(LOCAL_RAN_FILE['SID Device']).parse('Sheet1', skiprows=2)
	siddevices = siddevices.sort_values(by=['Domain']).reindex()
	siddevices['Test'] = 1
	cellDeviceListDf = cellDeviceListDf.rename(columns={'Parent ID': 'Device ID'})
	sidrandevices = siddevices.loc[((siddevices['NE Type'] == 'BT') | (siddevices['NE Type'] == 'NB') | (siddevices['NE Type'] == 'LT') | (siddevices['NE Type'] == 'TD'))]
	
	deviceVariance = pd.merge(cellDeviceListDf, sidrandevices, on=['Device ID'], how='left')
	deviceVariance = deviceVariance[deviceVariance['Test'].isnull()]
	deviceVariance = deviceVariance[['Device ID', 'Parent DN']]
	
	return deviceVariance
	
#---------------------------
def getDeviceOfVarianceFromRANDeviceList(varianceDevices):
	colsToRename = {
		'DEVICE_ID': 'Device ID', 'EMS_DEVICE_ID': 'EMS Device ID', 'DEVICE_ALIAS': 'Device Alias', 'DEVICE_IP': 'Device IP', 'EMS_ID': 'EMS ID',
		'VENDOR_ID': 'Vendor ID', 'NE_TYPE': 'NE Type', 'MODEL': 'Model', 'HW_DESC': 'Hardware Description', 'FNC_DESC': 'Functional Description',
		'PARENT_ID': 'Parent Device ID', 'PARENT_DN': 'ParentDN', 'SITE_ID': 'Site ID', 'DEVICE_STATE': 'Device State', 'SW_VER': 'Software Version',
		'INT_DATE': 'Integration Date', 'EOS': 'End of Support', 'TSA_SCOPE': 'TSA Scope', 'PROD_ID': 'Product ID', 'SERIAL_NO': 'Serial Number',
		'FREQ_TXRX': 'FREQ (TX/RX)','HW_CAP': 'Hardware Capacity', 'DOMAIN': 'Domain', 'NE_OWNER': 'NE Owner', 'TX_CLUSTER': 'TX Clusterimg', 'TX_TYPE': 'TX Type',
		'NAT_SP_CODE': 'NATSPCODE', 'ADMIN_STATE': 'Admin State', 'IUBCE_DL_LIC': 'IUBCE DL LIC', 'IUBCE_UL_LIC': 'IUBCE UL LIC', 'S1CU_LIC': 'S1CU LIC'
	}
	nmsrandevice = dataframe(LOCAL_RAN_FILE['NMS Device'])
	nmsrandevice.setColsToRename(colsToRename)
	nmsrandevice.createCSVDataframe()
	nmsrandevice.df['Cluster Region'] = ''
	nmsrandevice.df['Cluster Sub Region'] = ''
	nmsrandevice.df['Cluster Province'] = ''
	nmsrandevice.df['Cluster City'] = ''
	nmsrandevice.df['MW HUB'] = ''
	
	varianceDevices = varianceDevices.drop_duplicates('Device ID')
	print("Count variance from SID: {}".format(len(varianceDevices.index)))
	
	matchCellVarianceAndNMSDevice = pd.merge(nmsrandevice.df, varianceDevices, on=['Device ID'], how='inner')
	matchCellVarianceAndNMSDevice = matchCellVarianceAndNMSDevice.drop(['Parent DN'], axis=1)
	#----------------------
	matchCellVarianceAndNMSDevice = getNEOwnerOfDevicesFromNOCFADB(matchCellVarianceAndNMSDevice)
	#----------------------
	print("Count of variance that matched in NMS Device: {}".format(len(matchCellVarianceAndNMSDevice.index)))
	
	nmsrandevice.df.insert(2, 'Test', 1)
	inVarianceNotInNMSDevice = pd.merge(varianceDevices, nmsrandevice.df, on=['Device ID'], how='left')
	inVarianceNotInNMSDevice = inVarianceNotInNMSDevice[inVarianceNotInNMSDevice['Test'].isnull()]
	inVarianceNotInNMSDevice = inVarianceNotInNMSDevice.drop(['Test'], axis=1)
	print("Number of missing variance in NMS Device: {}".format(len(inVarianceNotInNMSDevice.index)))
	saveDataframeToFile(inVarianceNotInNMSDevice, 'DevicesInCellListNotInSIDandNMS_{}'.format(timestr))
	
	return matchCellVarianceAndNMSDevice
	
#---------------------------
def getNEOwnerOfDevicesFromNOCFADB(deviceDf):
	
	siteAor = dataframe()
	siteDB = database('10.150.20.101', 'ovim', 'ovim', 'smartSiteDB')
	print('Getting NE Owner of devices with blank values.')
	print('Connecting to NOCFA database...', flush=True, end="")
	if siteDB.connect():
		print('[OK]')
		siteAor.createSQLResultDataframe("select siteid, toc_aor from smart_site", siteDB.conn)
		deviceDf = deviceDf.fillna("")
		for i, row in deviceDf.iterrows():
			if row['NE Owner'] == "":
				result_aor = siteAor.df[siteAor.df['siteid'] == row['Site ID']]
				if len(result_aor.index) > 0:
					for ix, rw in result_aor.iterrows():
						deviceDf.at[i, 'NE Owner'] = rw['toc_aor']
				else:
					deviceDf.at[i, 'NE Owner'] = ''
	else:
		print('[FAILED]')
		
	return deviceDf
	
#---------------------------
def getNonMobileRANDevices():
	print('Get all non RAN mobile devices...')
	siddevices = pd.ExcelFile(LOCAL_RAN_FILE['SID Device']).parse('Sheet1', skiprows=2)
	siddevices = siddevices.sort_values(by=['Domain']).reindex()
	
	nonRanSIDDevices = siddevices.loc[((siddevices['NE Type'] != 'BT') & (siddevices['NE Type'] != 'NB') & (siddevices['NE Type'] != 'LT') & (siddevices['NE Type'] != 'TD'))]
	
	#added 11/24/2020 for DEA-server which has an NE Type of "LT"
	additionalNonRANSIDDevices = siddevices.loc[((siddevices['NE Type'] == 'LT') & (siddevices['Domain'] == 'TSC_L2_INTL'))]
	
	finalNonRANSIDDevices = pd.concat([nonRanSIDDevices, additionalNonRANSIDDevices],ignore_index=True).reset_index(drop=True)
	
	#return nonRanSIDDevices
	return finalNonRANSIDDevices
	
#---------------------------
def saveDataframeToFile(df, filename, start_row = 0):
	successfullySavedFile = False
	try:
		print('Writing dataframe to file...', end="")
		writer = pd.ExcelWriter("{}.xlsx".format(OUTPUT_DIR+filename))
		df.to_excel(writer, sheet_name='Sheet1', startrow = start_row, index=False)
		writer.save()
		successfullySavedFile = True
		print('[File saved]')
	except:
		print('[File not saved]')
		print('An error occured while trying to save the file.')
	print("")
	
	return successfullySavedFile
	
#---------------------------
def getSIDvsNOCFA_OLD(siddf, nocfadf):
	varianceDf = dataframe()
	pattern = re.compile('\d+')
	
	print(nocfadf)
	ctr = 1
	total = len(siddf.index)
	
	for i, row in siddf.iterrows():
		if not pd.isna(row['Band']):
			if not row['Band'].isnumeric():
				matchObj = pattern.search(row['Band'])
				row['Band'] = matchObj[0]
			row['Band'] = int(row['Band'])
		else:
			row['Band'] = ''
			
		
		searchDf = nocfadf.loc[(nocfadf['siteid'] == str(row['Site'])) & (nocfadf['band'] == row['Band']) & (nocfadf['tech'] == str(row['SUBDOMAIN']))]
		print(searchDf)
		#if len(searchDf.index) > 0:
		#	print(i, row['Cell Name'], row['Parent ID'], row['Site'], row['Band'], row['Tech'], row['SUBDOMAIN'], 'OK')
		#else:
		#	print(i, row['Cell Name'], row['Parent ID'], row['Site'], row['Band'], row['Tech'], row['SUBDOMAIN'], 'NOT FOUND')
	
if __name__ == "__main__":
	starTime = datetime.now()
	print("\nRun Datetime: {}".format(starTime))
	print("---------------------")
	print("Project SID match FA.")
	print("---------------------\n")
	
	main()
	
	endTime = datetime.now()
	deltaDt = endTime - starTime
	print(deltaDt)
	print("<<<<< End of Script >>>>>")
