#!/usr/bin/env /home/jcm/env/sidxfa/bin/python

import cx_Oracle

class Oracle(object):

    def connect(self, username, password, hostname, port, servicename):
        """ Connect to the database. """

        try:
            self.db = cx_Oracle.connect(username, password, hostname + ':' + port + '/' + servicename)
            # If the database connection succeeded create the cursor
            self.cursor = self.db.cursor()
            self.connection = True

        except cx_Oracle.DatabaseError as exc:
            self.connection = False
            error, = exc.args
            print("Oracle-Error-Code:", error.code)
            print("Oracle-Error-Message:", error.message)
            # Log error as appropriate
            #raise

    def disconnect(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """

        try:
            if self.connection:
                self.cursor.close()
                self.db.close()
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=None, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """

        try:
            if self.connection:
                self.cursor.execute(sql)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            raise

        # Only commit if it-s necessary.
        if commit:
            self.db.commit()