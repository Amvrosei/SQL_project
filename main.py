import psycopg2
import pandas as pd
from sqlalchemy import create_engine, types
import os
import sys

from py_scripts.all_func_files import get_files, copy_file_and_remove

from py_scripts.terminals_scripts import *
from py_scripts.transactions_scripts import *
from py_scripts.passport_blacklist_scripts import *
from py_scripts.result_report import *

# указываем папку с файлами ('files')
folder_name = 'files'
dir_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),folder_name)

engine = create_engine(
    f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}'
    )

def csv2sql(engine, path, table_name, schema_name): 
    with engine.begin() as conn:
        df = pd.read_csv(path, delimiter=';')
        df['amount'] = df['amount'].str.replace(',','.').apply(pd.to_numeric)
        df.to_sql(name = table_name, con = conn, schema = schema_name, if_exists="replace", index=False)  
    
def xlsx2sql(engine, path, table_name, schema_name):
    with engine.begin() as conn:
        df = pd.read_excel(io=path)
        df.to_sql(name = table_name, con = conn, schema = schema_name, if_exists="replace", index=False)   

def write_files_to_stg_tables(files):   
    for key in files.keys():
        for file in files[key]: 
            
            file_path = file[1]
            extension = os.path.splitext(file[1])[1]

            if extension == '.txt':
                csv2sql(engine = engine, path = file_path, table_name = f"stg_{key}", schema_name="final_proj")
                copy_file_and_remove(file_path)
            elif extension == '.xlsx':
                xlsx2sql(engine = engine, path = file_path, table_name = f"stg_{key}", schema_name="final_proj")
                copy_file_and_remove(file_path)

# ============== таблицы ==================
# записываем в таблички STG данный из файлов
files = get_files(dir_path)
write_files_to_stg_tables(files)

# ============== таблицы terminal, transactions, passport =================
create_dwh_dim_terminals_hist()
create_stg_terminals_new_rows()
create_stg_terminals_deleted_rows()
create_stg_terminals_updated_rows()
update_dwh_dim_terminals_hist()

create_dwh_dim_transactions_hist()
create_stg_transactions_new_rows()
create_stg_transactions_deleted_rows()
create_stg_transactions_updated_rows()
update_dwh_dim_transactions_hist()

create_dwh_dim_passport_blacklist_hist()
create_stg_passport_blacklist_new_rows()
create_stg_passport_blacklist_deleted_rows()
create_stg_passport_blacklist_updated_rows()
update_dwh_dim_passport_blacklist_hist()

# ================== витрина =================
stg_antifraud()
new_rows = antifraud_new()
print(new_rows)
insert_stg_antifraud(new_rows)

create_dwh_dim_antifraud_hist()
create_stg_antifraud_new_rows()
create_stg_antifraud_updated_rows()
update_dwh_dim_antifraud_hist()

rep_fraud()

# удаление временных таблиц
remove_stg_antifraud_tables()
remove_stg_terminals_tables()
remove_stg_transactions_tables()
remove_stg_passport_blacklist_tables()


