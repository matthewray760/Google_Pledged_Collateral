import pyodbc as py
import pandas as pd
from sql import fidelity_execute_query
from configurations.manager_dataclass import Fidelity_BR_Data
from mappings.map_fidelity import colp_colh_mappings
from configurations.config import USE_SQL_FIDELITY,BEGIN_BALANCE_FIDELITY,FILENAME_FIDELITY


clearwater_main_sleeve = '274045'
Fund = 'AEMW'


def run_fidelity(begin_date,end_date):
    
    USE_SQL = USE_SQL_FIDELITY
    BEGIN_BALANCE = BEGIN_BALANCE_FIDELITY
    FILENAME = FILENAME_FIDELITY
    pathway_insert = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python_vscode\Fidelity\Insert\{FILENAME}.xlsx'


    if USE_SQL == True:
        data= fidelity_execute_query(begin_date=begin_date,end_date=end_date)[0]
    else:
        data = pd.read_excel(pathway_insert)



    ###### start the transformation process
    df = pd.DataFrame(data)





    #Drop and rename columns
    df = df.drop(columns='Manager')
    df.rename(columns = {'Recon Date': 'recon_date'}, inplace = True)
    df.rename(columns = {'COLLATERAL_PURPOSE': 'collateral_purpose'}, inplace = True)
    df['Current_Par'] = df['Current_Par'].astype(int)

    df['collateral_type'] = df['Current_Par'].apply(lambda x: 'COLP' if x > 0 else 'COLH')






    #Create Purpose Indicator from 
    purpose_indicator_mapping = {
        'Over the Counter' :'OTC',
        'Futures':'F'
    }

    counterparty_code_mapping = {
        'BANK OF AMERICA NA' :'BOA',
        'MORGAN STANLEY & CO LLC': 'MSC',
        'ROYAL BANK OF CANADA' : 'RBOC',
        'BNP PARIBAS' : 'BNP',
        'JPMORGAN CHASE BANK NA' : 'JPM',
        'STATE ST BK & TR CO BOSTON' : 'SSB',
        'BROWN BROTHERS HARRIMAN & CO' : 'BB',
        'GOLDMAN SACHS BANK USA': 'GS',
        'CITIBANK NA': 'CB'

    }

    df['purpose_indicator'] = df['collateral_purpose'].map(purpose_indicator_mapping)
    df['counterparty_code'] = df['Counterparty'].map(counterparty_code_mapping)



    #Create Keys and reorder columns
    df['key'] = df['Cusip'] + '-' + df['counterparty_code'] + '-' + df['purpose_indicator'] + '-' + df['collateral_type']
    df = df[['recon_date','key', 'Counterparty', 'counterparty_code','purpose_indicator','Current_Par','collateral_type']]



    #calculate being/end date balance changes
    filter_df = df.copy()


    #

    pivot_df = filter_df.pivot(index = 'key', columns= 'recon_date', values = 'Current_Par')
    pivot_df = pivot_df.fillna(0)

    if BEGIN_BALANCE ==True:
        pivot_df['balance_change'] = pivot_df[end_date] - pivot_df[begin_date]
    else:
        pivot_df['balance_change'] = pivot_df[end_date]

    pivot_df = pivot_df.reset_index()


    ###Create dataframe for summary


    df_summary = pivot_df.copy()

    df_summary['Account_Code'] = df_summary['key'].str.split('-').str[-3:].str.join('-')
    df_summary['Cusip'] = df_summary['key'].str.split('-').str[:1].str.join('-')

    df_summary = df_summary.drop(columns ='key')
    df_summary = df_summary.reset_index()

    if BEGIN_BALANCE == True:
        df_summary.columns = ['Index', 'begin balance', 'end balance', 'balance_change', 'Account_Code','Cusip']
    else: 
        df_summary.columns = ['Index', 'end balance', 'balance_change', 'Account_Code','Cusip']

    df_summary = df_summary.drop(columns = 'Index')

    df_summary['collateral_type'] = df_summary['end balance'].apply(lambda x: 'COLP' if x > 0 else 'COLH')



    #Initiate direct transfer df
    df_bt = pivot_df.copy()
    df_bt.reset_index(inplace= True)

    df_bt['collateral_type'] = df_bt['key'].str.split('-').str[-1:].str.join('-')
    df_bt = df_bt[df_bt['collateral_type'] == 'COLP']

    df_bt['Account_Code'] = df_bt['key'].str.split('-').str[-3:].str.join('-')


    df_bt['Cusip'] = df_bt['key'].str.split('-').str[:1].str.join('-')
    df_bt = df_bt.drop(columns = 'key')

    df_bt['Broker_Account'] = None
    df_bt['Investment_Account'] = clearwater_main_sleeve


    df_bt['Broker_Account'] = df_bt['Account_Code'].map(colp_colh_mappings)




    df_bt['from_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] < 0 else row['Investment_Account'], axis=1)
    df_bt['to_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] > 0 else row['Investment_Account'], axis=1)

    #df_bt.loc[:,'from_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] < 0 else row['Investment_Account'], axis=1)
    #df_bt.loc[:,'to_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] > 0 else row['Investment_Account'], axis=1)


    df_bt = df_bt[df_bt['balance_change'] != 0] # drop rows where balance change equals zero

    df_bt['Asset Id'] = df_bt['Cusip']
    df_bt['Entry Date'] = end_date
    df_bt['Dest Account Id'] = df_bt['to_account']
    df_bt['Source Account Id'] = df_bt['from_account']
    df_bt['Units'] = df_bt['balance_change']

    df_bt = df_bt[['Entry Date','Asset Id', 'Source Account Id', 'Dest Account Id','Units']]



    # one-sided TRNI
    df_in = pivot_df.copy()
    df_in.reset_index(inplace= True)

    df_in['collateral_type'] = df_in['key'].str.split('-').str[-1:].str.join('-')
    df_in = df_in[df_in['collateral_type'] == 'COLH']

    df_in['Account_Code'] = df_in['key'].str.split('-').str[-3:].str.join('-')
    df_in['Cusip'] = df_in['key'].str.split('-').str[:1].str.join('-')
    df_in = df_in.drop(columns = 'key')

    df_in['Broker_Account'] = None
    df_in['Broker_Account'] = df_in['Account_Code'].map(colp_colh_mappings)

    df_in = df_in[df_in['balance_change'] != 0]
    df_in = df_in[df_in['balance_change'] < 0] # drop rows where balance change equals zero

    df_in['Account_ID'] = df_in['Broker_Account']
    df_in['Tran Type'] = 'TRNI'
    df_in['Asset_ID'] = df_in['Cusip']
    df_in['Transfer Date'] = end_date
    df_in ['Orig Units'] = df_in['balance_change']
    df_in['Transfer Price'] = 100
    df_in['Inventory Type'] = 'Short Position'
    df_in['Collateral Broker'] = ''
    df_in['GAAP Orig Trade Date'] = end_date
    df_in['GAAP Orig Settle Date'] = end_date
    df_in['GAAP Orig Cost'] = ''
    df_in['GAAP Orig Price'] = 100
    df_in['GAAP Accrued'] = 0
    df_in['GAAP Commission'] = 0
    df_in['GAAP Amort Start Cost'] = ''
    df_in['GAAP Amort Start Price'] = ''
    df_in['GAAP Amort Start Date'] = end_date


    df_in = df_in[['Account_ID','Tran Type','Asset_ID','Transfer Date','Orig Units','Transfer Price','Inventory Type', 'Collateral Broker', 'GAAP Orig Trade Date','GAAP Orig Settle Date',
                'GAAP Orig Cost',
                    'GAAP Orig Price',
                    'GAAP Accrued',
                    'GAAP Commission',
                    'GAAP Amort Start Cost',
                    'GAAP Amort Start Price',
                    'GAAP Amort Start Date'
                ]]



    #one-sided TRNO
    df_ou = pivot_df.copy()
    df_ou = df_ou.reset_index()

    df_ou['collateral_type'] = df_ou['key'].str.split('-').str[-1:].str.join('-')
    df_ou = df_ou[df_ou['collateral_type'] == 'COLH']

    df_ou['Account_Code'] = df_ou['key'].str.split('-').str[-3:].str.join('-')
    df_ou['Cusip'] = df_ou['key'].str.split('-').str[:1].str.join('-')
    df_ou = df_ou.drop(columns = 'key')

    df_ou['Broker_Account'] = None
    df_ou['Broker_Account'] = df_ou['Account_Code'].map(colp_colh_mappings)

    df_ou = df_ou[df_ou['balance_change'] != 0] # drop rows where balance change equals zero
    df_ou = df_ou[df_ou['balance_change'] > 0] 


    df_ou['Account_ID'] = df_ou['Broker_Account']
    df_ou['Tran Type'] = 'TRNO'
    df_ou['Asset_ID'] = df_ou['Cusip']
    df_ou['Transfer Date'] = end_date
    df_ou['Orig Units'] = df_ou['balance_change']
    df_ou['Transfer Price'] = ''
    df_ou['Inventory Type'] = 'SHORT'
    df_ou['Collateral Broker'] = ''
    df_ou['Market Value Transfer'] = ''

    df_ou = df_ou[['Account_ID','Tran Type','Asset_ID','Transfer Date', 'Orig Units', 'Transfer Price', 'Inventory Type', 'Collateral Broker', 'Market Value Transfer']]




    #Check for Null Values
    if df_bt.isnull().values.any():
        print("Fidelity: Missing account ID for broker-purpose indicator pair. Please follow SOP - https://cwan.atlassian.net/wiki/spaces/SOP/pages/690618738/Missing+Broker-Purpose+Indicator+pairs")

    if df_in.isnull().values.any():
        print("Fidelity: Missing account ID for broker-purpose indicator pair. Please follow SOP - https://cwan.atlassian.net/wiki/spaces/SOP/pages/690618738/Missing+Broker-Purpose+Indicator+pairs")

    if df_ou.isnull().values.any():
        print("Fidelity: Missing account ID for broker-purpose indicator pair. Please follow SOP - https://cwan.atlassian.net/wiki/spaces/SOP/pages/690618738/Missing+Broker-Purpose+Indicator+pairs")

    return Fidelity_BR_Data(
        bt=df_bt,
        sum=df_summary,
        chin=df_in,
        chou=df_ou
    )












        
