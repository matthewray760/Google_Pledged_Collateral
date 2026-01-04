import pandas as pd
import numpy as np
import openpyxl
import xlsxwriter



def to_excel(end_date,sum_10889,bt_10889, sum_11399,bt_11399,sum_blackrock,bt_blackrock,chin_blackrock,chou_blackrock,sum_fidelity, bt_fidelity, chin_fidelity, chou_fidelity):

    sheets = {
        "10889 Summary": sum_10889,
        "10889 Bulk Entry": bt_10889,
        "11399 Summary": sum_11399,
        "11399 Bulk Entry" : bt_11399,
        "Blackrock Summary" : sum_blackrock,
        "Blackrock Bulk Entry": bt_blackrock,
        "Blackrock COHL TRNI": chin_blackrock,
        "Blackrock COHL TRNO": chou_blackrock,
        "Fidelity Summary" : sum_fidelity,
        "Fidelity Bulk Entry": bt_fidelity,
        "Fidelity COHL TRNI": chin_fidelity,
        "Fidelity COHL TRNO": chou_fidelity
    }


    pathway_output = fr'C:\Users\matthewray\OneDrive - Clearwater\Desktop\Google\python\pledged_collateral\outputs\{end_date}.xlsx'


    with pd.ExcelWriter(engine='openpyxl',path=pathway_output) as writer:
        for sheet_name,df in sheets.items():
            df.to_excel(writer,sheet_name= sheet_name,index=False)

            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_length = 0
                column = column_cells[0].column_letter
                for cell in column_cells:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                adjusted_width = (max_length + 6)
                worksheet.column_dimensions[column].width = adjusted_width


    
    
    
    

