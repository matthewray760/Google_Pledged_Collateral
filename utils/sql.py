import pyodbc as py
import pandas as pd
import datetime
import tkinter as tk




def blk_execute_query(begin_date,end_date):
        # Set up the database connection parameters
        server = 'P01-01-AG-004'
        database = 'custodydata'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207&'
        driver = '{ODBC Driver 17 for SQL Server}'
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
        DECLARE @EndDate DATE = '{end_date}';
        DECLARE @BeginDate DATE = '{begin_date}';


        SELECT Cusip, Counterparty, CAST(REPLACE(REPLACE(Current_Par, '.00', ''), ',', '') AS INT) AS Current_Par ,Agreement_Type,  dbo.DateFromGlobalDayNumber(_ReconDate)[Recon Date], 'BLACKROCK' as 'Manager'
        FROM rawdata.FileModel_9965
        WHERE 1=1
            and (_ReconDate = dbo.DateToGlobalDayNumber(@EndDate) OR _ReconDate = dbo.DateToGlobalDayNumber(@BeginDate))
            and Tran_Type = 'COLL'
            
        order by _ReconDate DESC
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df, print("Blackrock: SQL executed within the timeout period")
    

def fidelity_execute_query(begin_date,end_date):
        # Set up the database connection parameters
        server = 'P01-01-AG-004'
        database = 'custodydata'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207&'
        driver = '{ODBC Driver 17 for SQL Server}'
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
        DECLARE @EndDate DATE = '{end_date}';
        DECLARE @BeginDate DATE = '{begin_date}';


        SELECT CUSIP_PRICING_NUMBER [Cusip], COUNTRY_OF_RISK [Counterparty], SHARES_PAR [Current_Par],  dbo.DateFromGlobalDayNumber(_ReconDate)[Recon Date], 'FIDELITY' as 'Manager', COLLATERAL_PURPOSE
        FROM rawdata.FileModel_17419
        WHERE 1=1
	        and (_ReconDate = dbo.DateToGlobalDayNumber(@EndDate) OR _ReconDate = dbo.DateToGlobalDayNumber(@BeginDate))
	        and INSTRUMENT_TYPE_DESC = 'Collateral'
	        and INSTRUMENT_TYPE_CD = 'Securities'
	
            
        order by _ReconDate DESC
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df, print("Fidelity: SQL executed within the timeout period")
