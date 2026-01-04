import pyodbc as py
import pandas as pd
import numpy as np
from datetime import date
from sql import blk_execute_query
from configurations.manager_dataclass import Fidelity_BR_Data
from mappings.map_blackrock import colp_mappings,colh_mappings
from configurations.config import USE_SQL_BLACKROCK,BEGIN_BALANCE_BLACKROCK,FILENAME_BLACKROCK


clearwater_main_sleeve = '194359'
Fund = 'AEMJ'



def run_blackrock(begin_date,end_date):
    USE_SQL = USE_SQL_BLACKROCK
    FILENAME= FILENAME_BLACKROCK
    BEGIN_BALANCE = BEGIN_BALANCE_BLACKROCK

    pathway_insert = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Python_vscode\Blackrock\Insert\{FILENAME}.xlsx'

    if USE_SQL == True:
        data = blk_execute_query(begin_date= begin_date, end_date=end_date)[0]
    else:
        data = pd.read_excel(pathway_insert)


    ###### start the transformation process
    df = pd.DataFrame(data)
    df['collateral_type'] = df['Current_Par'].apply(lambda x: 'COLP' if x > 0 else 'COLH')




    #Drop and rename columns
    #df = df.drop(columns='Manager')
    df.rename(columns = {'Recon Date': 'recon_date'}, inplace = True)



    #Create Purpose Indicator from Agreement Type
    conditions = {
        'ISDA' :'W',
        'TBA':'T'
    }

    df['purpose_indicator'] = df['Agreement_Type'].map(conditions)

    #Create Keys and reorder columns
    df['key'] = df['Cusip'] + '-' + df['Counterparty'] + '-' + df['purpose_indicator']
    df = df[['recon_date','key', 'Counterparty', 'Agreement_Type','purpose_indicator','Current_Par','collateral_type']]

    #calculate being/end date balance changes
    filter_df = df.copy()
    filter_df_colp = filter_df[filter_df['collateral_type'] == 'COLP']
    filter_df_colh = filter_df[filter_df['collateral_type'] == 'COLH']


    #Calculate beginning and ending balances

    begin_df_colp = filter_df_colp[filter_df_colp['recon_date'] == begin_date].copy()
    begin_df_colp.rename(columns = {'Current_Par': 'Begin_Balance'}, inplace = True)
    begin_balances = begin_df_colp.groupby(['key'])['Begin_Balance'].sum()


    end_df_colp = filter_df_colp[filter_df_colp['recon_date'] == end_date].copy()
    end_df_colp.rename(columns = {'Current_Par': 'End_Balance'}, inplace = True)
    end_balances = end_df_colp.groupby(['key'])['End_Balance'].sum()

    begin_df_colh = filter_df_colh[filter_df_colh['recon_date'] == begin_date].copy()
    begin_df_colh.rename(columns = {'Current_Par': 'Begin_Balance'}, inplace = True)
    begin_balances_colh = begin_df_colh.groupby(['key'])['Begin_Balance'].sum()


    end_df_colh = filter_df_colh[filter_df_colh['recon_date'] == end_date].copy()
    end_df_colh.rename(columns = {'Current_Par': 'End_Balance'}, inplace = True)
    end_balances_colh = end_df_colh.groupby(['key'])['End_Balance'].sum()



    #Combine and calculate balance changes (COLP)
    df_combined = pd.concat([begin_balances,end_balances], axis= 1)
    df_combined = df_combined.fillna(0)

    df_combined['balance_change'] = df_combined['End_Balance'] - df_combined['Begin_Balance']

    #Combine and calculate balance changes (COLH)
    df_combined_colh = pd.concat([begin_balances_colh,end_balances_colh], axis= 1)
    df_combined_colh = df_combined_colh.fillna(0)

    df_combined_colh['balance_change'] = df_combined_colh['End_Balance'] - df_combined_colh['Begin_Balance']

    concatenated_df = pd.concat([df_combined, df_combined_colh], axis=0)





    ###Create dataframe for summary

    #Split Key into broker-account codes
    df_summary = concatenated_df.copy()

    df_summary.reset_index(inplace= True)
    df_summary['Account_Code'] = df_summary['key'].str.split('-').str[-2:].str.join('-')
    df_summary['Cusip'] = df_summary['key'].str.split('-').str[:1].str.join('-')
    df_summary = df_summary.drop(columns =['key'])
    df_summary.set_index('Cusip', inplace = True)
    df_summary.columns = ['begin balance','end balance','balance_change','account code']


    if BEGIN_BALANCE == True:
        df_summary.columns = ['begin balance', 'end balance', 'balance_change', 'Account_Code']
    else: 
        df_summary.drop(columns= 'begin balance',inplace=True)
        df_summary.columns = ['end balance', 'balance_change', 'Account_Code']



    #Initiate Broker and Investment Account

    df_bt = df_combined.copy()
    df_bt.reset_index(inplace= True)
    df_bt['Account_Code'] = df_bt['key'].str.split('-').str[-2:].str.join('-')
    df_bt['Cusip'] = df_bt['key'].str.split('-').str[:1].str.join('-')
    df_bt = df_bt.drop(columns = 'key')

    df_bt['Broker_Account'] = None
    df_bt['Investment_Account'] = clearwater_main_sleeve


    #map to colp accounts
    df_bt['Broker_Account'] = df_bt['Account_Code'].map(colp_mappings)



    df_bt = df_bt[df_bt['balance_change'] != 0] # drop rows where balance change equals zero
    df_bt.loc[:,'from_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] < 0 else row['Investment_Account'], axis=1)
    df_bt.loc[:,'to_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['balance_change'] > 0 else row['Investment_Account'], axis=1)


    begin_date = pd.to_datetime(begin_date)
    end_date = pd.to_datetime(end_date)

    df_bt['Asset Id'] = df_bt['Cusip']
    df_bt.loc[:, 'Entry Date'] = df_bt.apply(lambda row: begin_date + pd.Timedelta(days=1) if row['to_account'] == clearwater_main_sleeve else end_date, axis=1)
    #df_bt['Entry Date'] = end_date
    df_bt['Dest Account Id'] = df_bt['to_account']
    df_bt['Source Account Id'] = df_bt['from_account']
    df_bt['Units'] = df_bt['balance_change']

    df_bt = df_bt[['Entry Date','Asset Id', 'Source Account Id', 'Dest Account Id','Units']]



    #Check for Null Values
    if df_bt.isnull().values.any():
        print("Blackrock: Missing account ID for broker-purpose indicator pair. Please follow SOP - https://cwan.atlassian.net/wiki/spaces/SOP/pages/690618738/Missing+Broker-Purpose+Indicator+pairs")


    df_bt['Source Account Id'] = df_bt['Source Account Id'].astype(int)
    df_bt.sort_values(by = ['Source Account Id'], ascending=False, inplace=True)


    ## Create TRNI

    df_in = df_combined_colh.copy()
    df_in.reset_index(inplace= True)



    df_in['Account_Code'] = df_in['key'].str.split('-').str[-2:].str.join('-')
    df_in['Cusip'] = df_in['key'].str.split('-').str[:1].str.join('-')
    df_in = df_in.drop(columns = 'key')

    df_in['Broker_Account'] = None
    df_in['Broker_Account'] = df_in['Account_Code'].map(colh_mappings)

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
    df_in['Loan Prepaid Interest'] = ''
    df_in['Loan Past Due Interest'] =''
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






    ### Create TRNO

    df_ou = df_combined_colh.copy()
    df_ou.reset_index(inplace= True)



    df_ou['Account_Code'] = df_ou['key'].str.split('-').str[-2:].str.join('-')
    df_ou['Cusip'] = df_ou['key'].str.split('-').str[:1].str.join('-')
    df_ou = df_ou.drop(columns = 'key')

    df_ou['Broker_Account'] = None
    df_ou['Broker_Account'] = df_ou['Account_Code'].map(colh_mappings)

    df_ou = df_ou[df_ou['balance_change'] != 0]
    df_ou = df_ou[df_ou['balance_change'] > 0] # drop rows where balance change equals zero





    df_ou['Account_ID'] = df_ou['Broker_Account']
    df_ou['Tran Type'] = 'TRNO'
    df_ou['Asset_ID'] = df_ou['Cusip']
    df_ou['Transfer Date'] = end_date
    df_ou['Orig Units'] = df_ou['balance_change']
    df_ou['Transfer Price'] = 100
    df_ou['Inventory Type'] = 'SHORT'
    df_ou['Collateral Broker'] = ''



    df_ou = df_ou[['Account_ID','Tran Type','Asset_ID','Transfer Date','Orig Units','Transfer Price','Inventory Type', 'Collateral Broker']]


    return Fidelity_BR_Data(
        bt=df_bt,
        sum=df_summary,
        chin=df_in,
        chou=df_ou
    )
