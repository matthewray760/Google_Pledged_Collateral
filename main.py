import pandas as pd
from accounts.m_10889 import run_10889
from accounts.m_11399 import run_11399
from accounts.m_blackrock import run_blackrock
from accounts.m_fidelity import run_fidelity
from excel_output import to_excel

to_excel_func = False

#### Parameters

#Pimco
BEGIN_DATE_PIMCO = '12-01-2025'# Dates match whats on filename
END_DATE_PIMCO ='12-31-2025' # Dates match whats on filename

FILENAME_10889 = 'Copy of 10889 as 12.01.25 to 12.31.25 CCI CCO Transactions'
FILENAME_11399 = 'Copy of 11399 as 12.01.25 to 12.31.25 CCI CCO Transactions'


#Blackrock and Fidelity
BEGIN_DATE_BRFI = '2025-11-28'  #This will be equal to the end date of previous month (not T+1 like Pimco)
END_DATE_BRFI = '2025-12-31'





def run_pipeline():
    pim_10899_dfs = run_10889(BEGIN_DATE_PIMCO,END_DATE_PIMCO,FILENAME_10889)
    pim_11399_dfs = run_11399(BEGIN_DATE_PIMCO,END_DATE_PIMCO,FILENAME_11399)
    blackrock_dfs = run_blackrock(BEGIN_DATE_BRFI,END_DATE_BRFI)
    fidelity_dfs = run_fidelity(BEGIN_DATE_BRFI,END_DATE_BRFI)

    if to_excel_func == True:
        to_excel(
            END_DATE_PIMCO,

            #Pimco
            pim_10899_dfs.sum, pim_10899_dfs.bt,
            pim_11399_dfs.sum, pim_11399_dfs.bt,
            
            #Blackrock
            blackrock_dfs.sum, blackrock_dfs.bt, blackrock_dfs.chin, blackrock_dfs.chou,

            #Fidelity
            fidelity_dfs.sum, fidelity_dfs.bt, fidelity_dfs.chin, fidelity_dfs.chou
            )
    else:
        print("Excel file not generated. 'to_excel_func' set to false False")






if __name__ == '__main__':
    run_pipeline()
