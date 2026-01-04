import pandas as pd
import numpy as np
from configurations.manager_dataclass import PimcoData
from mappings.map_11399 import colp_mappings


#Parameters
Account = '11399'
clearwater_main_sleeve = 216465
Fund = 'AEMN'


    
    
def run_11399(begin_date,end_date,filename):    
    pathway_insert = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Google\python\pledged_collateral\inputs\11399\{filename}.xlsx'

    print(pathway_insert)

    #Check pathway
    if '11399' not in pathway_insert:
        raise ValueError("Invalid pathway: '11399' not found in pathway")


    #Parse excel file into Pandas DataFrame
    data = pd.read_excel(pathway_insert,header = 1,)
    df = pd.DataFrame(data)


    #Drop and rename columns
    df['Maturity_y.n'] = np.where(df['Maturity Date of Underlying CUSIP'] <= end_date, 'yes', 'no')
    df = df.drop(columns = ['Maturity Date of Underlying CUSIP','Settle Date','Account','Broker Name','CSTC','Description','Currency of Settlement', 'Trn Price','Country No','Price Denom','CostProc_Int_Comm_SecFee USD','OTC Sw', 'Exchange Code', 'Exchange Code 2', 'Exchange Code 3','Ticket Date','Ticket No', 'Ticket Rev Code', 'Tag No', 'Bloomberg ID','BB Ticket No', 'Pairoff Number', 'Revision Codes', 'Tax Gain Loss', 'GMT Entry Date', 'Cancel Code', 'GMT Cancel Date', 'Lot Trade Date', 'Lot Tag Number', 'System Note', 'Exec Broker' ])
    df.rename(columns = {'Transaction Code': 'Transaction_Code'}, inplace = True)
    df.rename(columns = {'Broker Id': 'Broker_Id'}, inplace = True)
    df.rename(columns = {'Purpose Indicator': 'Purpose_Indicator'}, inplace = True)
    df.rename(columns = {'Trade Date': 'Trade_Date'}, inplace = True)


    #filter for only CCO and CCI transactions
    df = df[df['Transaction_Code'].isin(['CCO','CCI'])] 

    #Calculate Par amount based on CCO/CCI
    df['Balance'] = np.where(df['Transaction_Code'] == 'CCO',df['Par'] * 1, df['Par']*-1)
    df = df.drop(columns = 'Par')



    #Start key/value pairs
    df['Key'] = df['CUSIP'] + '-' + df['Broker_Id'] + '-' + df['Purpose_Indicator']

    #Check for MTY:
    df_maturity = df[df['Maturity_y.n'] == 'yes']
    mty_cusips = df_maturity.loc[df['Maturity_y.n'] == 'yes', 'CUSIP'].unique()
    csv_cusips = ', '.join(mty_cusips)

    #Calculate beginning and ending balances

    begin_df = df[df['Trade_Date']< begin_date].copy()
    begin_df.rename(columns = {'Balance': 'Begin_Balance'}, inplace = True)
    begin_balances = begin_df.groupby(['Key'])['Begin_Balance'].sum()


    end_df = df.copy()
    end_df = end_df[end_df['Trade_Date']<= end_date]
    end_df.rename(columns = {'Balance': 'End_Balance'}, inplace = True)
    end_balances = end_df.groupby(['Key'])['End_Balance'].sum()



    #Combine and calculate balance changes
    df_combined = pd.concat([begin_balances,end_balances], axis= 1)
    df_combined = df_combined.fillna(0)

    df_combined['Change'] = df_combined['End_Balance'] - df_combined['Begin_Balance']



    ##Create dataframe for summary

    #Split Key into broker-account codes
    df_combined.reset_index(inplace= True)
    df_combined['Account_Code'] = df_combined['Key'].str.split('-').str[-2:].str.join('-')

    df_ac = df_combined.copy()

    #Initiate Broker and Investment Account
    df_ac['Broker_Account'] = None
    df_ac['Investment_Account'] = clearwater_main_sleeve


    # Map to COLP Accounts
    df_ac['Broker_Account'] = df_ac['Account_Code'].map(colp_mappings)
    df_ac = df_ac[['Key','Begin_Balance','Change','End_Balance','Account_Code','Broker_Account','Investment_Account']]


    ##Create dataframe for bulk tran entry

    df_bt = df_ac.copy()
    df_bt = df_bt[df_bt['Change'] != 0] # drop rows where balance change equals zero
    df_bt['from_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['Change'] < 0 else row['Investment_Account'], axis=1)
    df_bt['to_account'] = df_bt.apply(lambda row: row['Broker_Account'] if row['Change'] > 0 else row['Investment_Account'], axis=1)



    df_bt['Asset_id']=  df_bt['Key'].str.split('-').str[:-2].str.join('-')


    df_bt.loc[:,'Entry_Date'] = end_date
    df_bt.loc[:,'Source_Account_Id'] = df_bt['from_account']
    df_bt.loc[:,'Dest_Account_Id'] = df_bt['to_account']
    df_bt['Entry_Date'] = pd.to_datetime(df_bt['Entry_Date'])
    df_bt['Entry_Date'] = df_bt['Entry_Date'].dt.date
    df_bt.loc[:, 'Entry_Date'] = df_bt.apply(lambda row:  begin_date if row['Dest_Account_Id'] == clearwater_main_sleeve else end_date, axis=1)
    df_bt.loc[df_bt['Asset_id'].isin(mty_cusips), 'Entry_Date'] = begin_date
    df_bt.loc[:,'Units'] = df_bt['Change']

    df_bt.drop(columns = ['Key','Begin_Balance','End_Balance', 'Change', 'Account_Code', 'Broker_Account', 'Investment_Account'], inplace = True)

    df_bt = df_bt[['Entry_Date','Asset_id','Source_Account_Id','Dest_Account_Id','Units']]

    #Print MTY Cusips:

    print("11399 MATURITY CUSIPs: " + csv_cusips)
    print()

    #Check for Null Values
    if df_bt.isnull().values.any():
        print("11399 COUNTERPARTY CHECK: Missing account ID for broker-purpose indicator pair. Please follow SOP - https://cwan.atlassian.net/wiki/spaces/SOP/pages/690618738/Missing+Broker-Purpose+Indicator+pairs")
    else:
        df_bt['Source_Account_Id'] = df_bt['Source_Account_Id'].astype(int)
        df_bt.sort_values(by = ['Source_Account_Id'], ascending=False, inplace=True)
        print("11399 COUNTERPARTY CHECK: There are no missing broker accounts")
    print()

    return PimcoData(
        bt=df_bt,
        sum=df_ac
    )


