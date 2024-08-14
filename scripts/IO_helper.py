import pandas as pd
import openpyxl
import xlsxwriter
import xlrd


def update_sheet(path: str, df: pd.DataFrame, *args, **kwargs):
    """
    update_sheet --updates existing excel sheet

    Args:
        path (str): path to excel file
        df (pd.DataFrame): dataframe used to update excelfile
    """

    with pd.ExcelWriter(path, mode=kwargs.get('mode', 'a'),
                        engine=kwargs.get('engine', 'openpyxl'),
                        if_sheet_exists=kwargs.get('if_sheet_exists', 'overlay')) as writer:

        df.to_excel(writer, sheet_name=kwargs.get(
            'sheet_name', 'Sheet1'), index=kwargs.get('index', False))
