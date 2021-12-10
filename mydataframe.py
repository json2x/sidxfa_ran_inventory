#!/usr/bin/env /home/jcm/env/sidxfa/bin/python
__author__ = 'JCMillan'

import numpy as np
import pandas as pd
import MySQLdb
from sqlalchemy import create_engine
import glob
import openpyxl
import os
import sys

class dataframe:

	df = None
	__sliceDtFrame = None
	__deltaDtFrame = None
	__sourceFile = None
	__colsToRename = None
	__sqlEngine = None
	
	notification = {}
	error = []
	
	#-------------------------------------
	def __init__(self, filePath = None):
		self.pd = pd
		self.notification = {'status': False}
		self.__colsToRename = {}
	
		if filePath:
			self.setSourceFile(filePath)
	
	#-------------------------------------
	def setSourceFile(self, filePath):
		if filePath and os.path.isfile(filePath):
			self.__sourceFile = filePath
			self.notification['status'] = False
		else:
			self.error = "Invalid file path."
			self.notification['status'] = False
			self.notification['text'] = self.error
		
		return self
	
	#-------------------------------------	
	def showSourceFile(self):
		
		return self.__sourceFile
		
	#-------------------------------------
	#def createExcelDataframe(self, sheetname=0, header=0, skiprows=None, skip_footer=0, index_col=None, \
	#	parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None, chunksize=None, \
	#	convert_float=True, has_index_names=False, converters=None):
	
	def createExcelDataframe(self, sheetname=0, header=0, skiprows=None):
	
		try:
			
			self.df = pd.ExcelFile(self.__sourceFile).parse(sheetname=sheetname, header=header, skiprows=skiprows)
			self.notification['status'] = True
			
			if self.__colsToRename:
				#self.__renameCols()
				self.renameCols()
			
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return self
		
	#-------------------------------------
	def createCSVDataframe(self, sep=',', skiprows=None):
		try:
			self.df = pd.read_csv(self.__sourceFile, sep=sep, skiprows=skiprows, error_bad_lines=False, dtype='unicode')
			
			if self.__colsToRename:
				#self.__renameCols()
				self.renameCols()
			
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return self

    #-------------------------------------
	def createSQLResultDataframe(self, sql, connection):
		try:
			self.df = pd.read_sql(sql, connection)
		
			if self.__colsToRename:
				#self.__renameCols()
				self.renameCols()
			
		except:
			#self.error = "Unexpected error {}".format(sys.exc_info()[0])
			self.error = sys.exc_info()
			
		return self
	
	#-------------------------------------
	def dataframeToSQLTable(self, table):
		try:
			if self.__sqlEngine:
				self.df.to_sql(name=table, con=self.__sqlEngine, if_exists = 'append', index=False)
			else:
				print('Database connection not set.')
		except:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			
			self.error = "Unexpected error: {}\nFile: {}\nLine No: {}\n".format(exc_type, fname, exc_tb.tb_lineno)	
		
		return self
	
	#-------------------------------------
	def setSqlEngine(self, creds):
		constr = 'mysql+mysqldb://{user}:{pwd}@{host}:{port}/{db}'.format(user=creds['user'], pwd=creds['pwd'], \
		host=creds['host'], port=creds['port'], db=creds['db'])
		self.__sqlEngine = create_engine(constr)
		
		return self

    #-------------------------------------
	def select(self, colList = []):
		try:
			if isinstance(colList, list):
				self.__sliceDtFrame = self.df[colList]
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return self.__sliceDtFrame
	
	#-------------------------------------
	def setColsToRename(self, objColsRename = {}):
		try:
			if objColsRename and isinstance(objColsRename, dict):
				self.__colsToRename = objColsRename
		
		except TypeError as Err:
			self.error = Err
		
		return self
			
	#-------------------------------------
	def renameCols(self):
		try:
			if len(self.df.index) > 0:
				self.df = self.df.rename(columns=self.__colsToRename)
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
	
		return self
		
	#-------------------------------------
	def fillColNullValues(self, colName, newValue):
		self.df[colName].fillna(newValue, inplace=True)
		
		return self

    #-------------------------------------
	def customSortDataframeByCol(self, colName, customOrder = [], ascending = True):
		if len(customOrder) > 0:
			self.df[colName] = pd.Categorical(self.df[colName], customOrder)
			
		if ascending:
			self.df = self.df.sort_values(colName)
		else:
			self.df = self.df.sort_values(colName, ascending=False)
		
		return self
		
	#-------------------------------------
	def reindexDataframe(self):
		try:
			self.df = self.df.reset_index(drop=True)
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return self
		
	#-------------------------------------
	def updateColValues(self, index, colName, value):
		try:
			self.df.at[index, colName] = value
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
		
		return self
		
	#-------------------------------------
	def dropDuplicateRows(self, cols = [], strKeep='first'):
		try:
			if len(cols) > 0:
				self.df.drop_duplicates(subset=cols, keep=strKeep, inplace=True)
			else:
				self.df.drop_duplicates(inplace=True)
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
		
		return self
		
	#-------------------------------------
	def getDuplicateRows(self, cols = [], strKeep='first'):
		df_original = self.df.copy()
		df_removed = None
		#try:
		if len(cols) > 0:
			df_temp = df_original.drop_duplicates(subset=cols, keep=strKeep)
		else:
			df_temp.df = df_original.drop_duplicates()
		
		df_removed = pd.concat([df_original, df_temp])
		df_removed = df_removed.reset_index(drop=True)
		df_gpby = df_removed.groupby(list(df_removed.columns))
		idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
		
		df_removed = df_removed.reindex(idx)
		#except:
		#	self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return df_removed
		
	#-------------------------------------
	def concatDataframes(self, dataframes = [], reindex = True):
		try:
			dataframes.insert(0, self.df)
			self.df = pd.concat(dataframes)
			if reindex:
				self.reindexDataframe()
		except:
			self.error = "Unexpected error {}".format(sys.exc_info()[0])
			
		return self
	
	'''
	#-------------------------------------
	def deltaVSIdenticalDataframes(self, inputDataframe, setDeltaToMainDataframe = False):
		
		self.__deltaDtFrame = pd.concat([self.df, inputDataframe])
		self.__deltaDtFrame = self.__deltaDtFrame.reset_index(drop=True)
		df_gpby = self.__deltaDtFrame.groupby(list(self.__deltaDtFrame.columns))
		idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
		
		self.__deltaDtFrame = self.__deltaDtFrame.reindex(idx)
		
		if setDeltaToMainDataframe:
			self.df = self.__deltaDtFrame
		
		return self
		
	#-------------------------------------
	def getDeltaDataframe(self):
		
		return self.__deltaDtFrame
	'''	
	
		
		
		