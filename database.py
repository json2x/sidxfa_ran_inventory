#!/usr/bin/env /home/jcm/env/sidxfa/bin/python
__author__ = 'JCMillan'

import MySQLdb as MySQL
import datetime
import sys

class database:

	database = {"host":"", "username":"", "password":"", "database":""}
	#dbChange = False
	
	__query = None
	__lastQuery = None
	__dbWrite = False
	
	connStatus = None
	conn = {}
	cursor = {}
	
	notification = {}
	error = []
	warning = []
	
	#-------------------------------------
	def __init__(self, host = None, username  = None, password = None, database = None):
		
		self.database['host'] = host
		self.database['username'] = username
		self.database['password'] = password
		self.database['database'] = database
		
		#Initialize __query as a dict
		self.__query = {}
		self.groupWhereOpenBool = False
		self.groupWhereCloseBool = False
		
		self.connStatus = False
		
	#-------------------------------------
	def connect(self):
		
		try:
			if self.database['database']:
				self.conn = MySQL.connect(self.database['host'], self.database['username'], self.database['password'], self.database['database'])
			else:
				self.conn = MySQL.connect(self.database['host'], self.database['username'], self.database['password'])
			self.cursor = self.conn.cursor()
			self.connStatus = True
			
		except MySQL.Error as Err:
			self.error = Err[1]
			
		except:
			self.error = "Unexpected error:", sys.exc_info()[0]
			
		if self.connStatus:
			self.notification = {"status":True, "text":"Connection successful"}
		else:
			self.notification = {"status":False, "text":"Connection unsuccessful"}
		
		return self.connStatus
			
	#-------------------------------------
	def disconnect(self):
		
		try:
			if self.connStatus and self.conn.open:
				self.conn.close()
				self.connStatus = False
				self.notification = {"status":True, "text":"Connection closed"}
		except:
			self.notification = {"status":False, "text":"Unexpected errpr {}".format(sys.exc_info()[0])}
		
		return not self.connStatus
		
	#Currently not used, update this to ensure proper order of query builder
	#-------------------------------------
	def organizeQuery(self):
		self.query = "" % (self.query.select, self.query.where, self.query.groupby, self.query.orderby, self.query.limit)
	
	#-------------------------------------	
	def properValue(self, value):
		try:
			if isinstance(value, int):
				properValue = "'{}'".format(int(value))
			elif isinstance(value, float):
				properValue = "'{}'".format(float(value))
			elif isinstance(value, str):
				if value == 'NULL':
					properValue = value
				else:
					properValue = "'%s'" % (self.conn.escape_string(value).decode("utf-8"))
			elif type(value) is datetime.datetime:
				properValue = "'{}'".format(value.strftime('%Y-%m-%d'))
			else:
				properValue = "'{}'".format(value)
				#properValue = "'%s'" % (self.conn.escape_string(value.encode("utf-8"))) #unicode_escape
				#properValue = "'%s'" % (self.conn.escape_string(value.decode("utf-8")))
				
		except:
			print('Unusual value type. {}'.format(type(value)))
			properValue = "'{}'".format(value)
			#if type(value) is datetime.datetime:
			#	properValue = "'{}'".format(value.strftime('%Y-%m-%d'))
			#else:
			#	properValue = "'{}'".format(value)
		
		return properValue
	
	#-------------------------------------
	#def query(self, query = None):
	def query(self):
		#if not query:
		query = ""
		if 'table' in self.__query:
			if 'insert' in self.__query:
				query = "INSERT INTO {} {}".format(self.__query['table'], self.__query['insert'])
			elif 'update' in self.__query:
				query = "UPDATE {} SET {}".format(self.__query['table'], self.__query['update'])
				if 'where' in self.__query:
					query += self.__query['where']
			elif 'delete' in self.__query:
				query = "DELETE FROM {}".format(self.__query['table'])
				if 'where' in self.__query:
					query += self.__query['where']
			else:
				try:
					if self.__query['select']:
						query = "SELECT {} FROM {}".format(self.__query['select'], self.__query['table'])
				except:
					query = "SELECT * FROM {}".format(self.__query['table'])
					
				if 'join' in self.__query:
					query += self.__query['join']
				if 'where' in self.__query:
					query += self.__query['where']
				if 'groupBy' in self.__query:
					query += self.__query['groupBy']
				if 'orderBy' in self.__query:
					query += self.__query['orderBy']
				if 'limit' in self.__query:
					query += self.__query['limit']
				
		return query
	
	#-------------------------------------
	def lastQuery(self):
		return self.__lastQuery
	
	#-------------------------------------
	def __execute(self):
		
		try:
			if self.connStatus:
				self.cursor.execute(self.query())
				self.conn.commit()
				self.notification = {"status":True, "text":"%d rows affected" % (self.cursor.rowcount)}
			else:
				self.notification = {"status":False, "text":"No database connection"}
				
		except MySQL.Error as Err:
			self.conn.rollback()
			self.notification = {"status":False, "text":Err}
			#self.error = Err[1]
		
		#Save last query executed and clear object live query placeholder
		self.__lastQuery = self.query()
		self.__query = {}
		
		return self
		
	#-------------------------------------
	def __toArray(self, cursor = None):
		
		if cursor is None:
			cursor = self.cursor
		
		resultArray = []
		try:
			if cursor.rowcount > 1:
				resultArray = cursor.fetchall()
			else:
				resultArray = cursor.fetchone()
		
		except:
			if len(self.error) < 1:
				self.error = "Unexpected error:", sys.exc_info()[0]
			
		return resultArray
	
	#-------------------------------------
	def get(self):
		resultArray = []
		self.__execute() #execute the object query
		if self.notification["status"]:
			resultArray = self.__toArray()
		
		return resultArray
		
	#-------------------------------------
	def table(self, table):
		if table:
			self.__query['table'] = table
		
		return self
		
	
	#Can be improve to use alias
	#-------------------------------------
	def select(self, lstFields):
		if lstFields:
			if isinstance(lstFields, list):
				self.__query['select'] = ', '.join(lstFields)
			else:
				self.__query['select'] = lstFields
		
		return self
	
	#-------------------------------------	
	def groupWhere(self, parenthesis):
		#parenthesis => 'open' or 'close'
		if parenthesis == 'open':
			self.groupWhereOpenBool = True
		if parenthesis == 'close':
			self.groupWhereCloseBool = True
	
	#-------------------------------------	
	def where(self, lstCondition = [], between = False):
		if lstCondition and isinstance(lstCondition, list):
			try:
				if self.__query['where']:
					if self.groupWhereOpenBool:
						self.__query['where'] += " AND ("
					else:
						self.__query['where'] += " AND"
			except:
				self.__query['where'] = " WHERE"
				
			if len(lstCondition) == 2:
				query = " {} = {}".format(lstCondition[0], self.properValue(lstCondition[1]))
			elif len(lstCondition) == 3 and between:
				query = " {} BETWEEN {} AND {}".format(lstCondition[0], self.properValue(lstCondition[1]), self.properValue(lstCondition[2]))
			elif len(lstCondition) == 3:
				query = " {} {} {}".format(lstCondition[0], lstCondition[1], self.properValue(lstCondition[2]))
			elif len(lstCondition) > 3:
				query = ""
				self.error = "Exceed max arguments supplied to where clause."
			elif len(lstCondition) < 2:
				query = ""
				self.error = "Insufficient arguments supplied to where clause."
				
			if self.groupWhereCloseBool:
				self.__query['where'] += query + " )"
			else:
				self.__query['where'] += query
			
			self.groupWhereOpenBool = False
			self.groupWhereCloseBool = False
		else:
			self.error = "Invalid argument in where."
			
		return self
		
	#-------------------------------------	
	def orWhere(self, lstCondition = []):
		if lstCondition and isinstance(lstCondition, list):
			try:
				if self.__query['where']:
					if self.groupWhereOpenBool:
						self.__query['where'] += " OR ("
					else:
						self.__query['where'] += " OR"
			except:
				self.__query['where'] = " WHERE"
				
			if len(lstCondition) == 2:
				query = " {} = {}".format(lstCondition[0], self.properValue(lstCondition[1]))
			elif len(lstCondition) == 3:
				query = " {} {} {}".format(lstCondition[0], lstCondition[1], self.properValue(lstCondition[2]))
			elif len(lstCondition) > 3:
				query = ""
				self.error = "Exceed max arguments supplied to orWhere clause."
			elif len(lstCondition) < 2:
				query = ""
				self.error = "Insufficient arguments supplied to orWhere clause."
		
			#self.__query['where'] += query
			if self.groupWhereCloseBool:
				self.__query['where'] += query + " )"
			else:
				self.__query['where'] += query
			
			self.groupWhereOpenBool = False
			self.groupWhereCloseBool = False
		
		else:
			self.error = "Invalid argument in orWhere."
			
		return self
		
	#-------------------------------------	
	def groupBy(self, lstFields = []):
		if lstFields:
			if isinstance(lstFields, list):
				query = " GROUP BY %s" % (', '.join(lstFields))
			else:
				query = " GROUP BY %s" % (lstFields)
			
			self.__query['groupBy'] = query
			
		return self
	
	#-------------------------------------	
	def orderBy(self, lstFields = [], order = "ASC"):
		if lstFields:
			order = order.upper()
			if not (order and (order == "ASC" or order == "DESC")):
				self.error = "Invalid order argument in orderBy {}" .format(order)
		
			if isinstance(lstFields, list):
				query = " ORDER BY %s %s" % (', '.join(lstFields), order)
			else:
				query = " ORDER BY %s %s" % (lstFields, order)
		
			self.__query['orderBy'] = query
			
		return self
	
	#-------------------------------------	
	def limit(self, limit = None, offset = None):
		try:
			if limit:
				self.__query['limit'] = " LIMIT %d" % (limit)
				if offset:
					self.__query['limit'] += " OFFSET %d" % (limit)
		except:
			self.error = "Unexpected error:", sys.exc_info()[0]
			
		return self
		
	#-------------------------------------	
	def join(self, table, onFields = [], type = "INNER"):
		if table and onFields:
			query = " %s JOIN %s" % (type, table)
			
			if len(onFields) == 2:
				query += " ON {} = {}".format(onFields[0], onFields[1])
			elif len(onFields) == 3:
				query += " ON {} {} {}".format(onFields[0], onFields[1], onFields[2])
			elif len(onFields) > 3:
				query = ""
				self.error = "Exceed max arguments supplied to orWhere clause."
			elif len(onFields) < 2:
				query = ""
				self.error = "Insufficient arguments supplied to orWhere clause."
			
			self.__query['join'] = query
		
		return self
	
	#-------------------------------------	
	def insert(self, objValues = {}):
		
		if len(objValues) > 0:
			strValues = ""
			strFields = ""
			for key, value in objValues.items():
				strFields += "`{}`, ".format(key)
				strValues += "{}, ".format(self.properValue(value))
					
			#query = "%s VALUES(%s)" % (strFields, strValues[:-2])
			query = '({}) VALUES({})'.format(strFields[:-2], strValues[:-2])
			self.__query['insert'] = query
			self.__execute()
			
		return self
	
	#-------------------------------------
	def update(self, objValues = {}):
		if objValues:
			query = ""
			for key, value in objValues.iteritems():
				query += "{} = {}, ".format(key, self.properValue(value))
			
			self.__query['update'] = query[:-2]
			self.__execute()
		
		return self
	
	#-------------------------------------	
	def lastInsertedId(self):
		try:
			id = self.cursor.lastrowid
		except:
			id = None
		
		return id
	
	#-------------------------------------
	def delete(self):
		self.__query['delete'] = True
		self.__execute()
		
		return self
		
#========== END OF CLASS ===========

