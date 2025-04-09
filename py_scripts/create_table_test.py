#import psycopg2
import pandas as pd
from sqlalchemy import create_engine, types
import os
import sys


conf = {
    "host":"localhost",
    "database":"sdb",
    "user":"postgres",
    "password":"1989",
    "port":"5432"
}

engine = create_engine(
    f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}'
    )

# def csv2sql(engine, path, table_name, schema_name): 
#     with engine.begin() as conn:
#         df = pd.read_csv(path, delimiter=';')
#         df.to_sql(name = table_name, con = conn, schema = schema_name, if_exists="replace", index=False)  
# #csv2sql ('files/transactions_03032021.txt', conf=conf, table_name='temp', schema_name= "final_proj")
    
# def xlsx2sql(engine, path, table_name, schema_name):
#     with engine.begin() as conn:
#         df = pd.read_excel(io=path)
#         df.to_sql(name = table_name, con = conn, schema = schema_name, if_exists="replace", index=False)   

def csv2sql(engine, path, table_name, schema_name): 
    dtype = {
        'transaction_id' : types.NUMERIC(),
        'transaction_date' : types.VARCHAR(length=128), 
        'amount' : types.Numeric(),
        'card_num' : types.VARCHAR(length=128),
        'oper_type' : types.VARCHAR(length=128), 
        'oper_result' : types.VARCHAR(length=128), 
        'terminal' : types.VARCHAR(length=128)
    }

    df = pd.read_csv(path, delimiter=';')
    df['amount'] = df['amount'].str.replace(',','.').apply(pd.to_numeric)
    with engine.begin() as conn:
        df.to_sql(name = table_name, dtype=dtype, con = conn, schema = schema_name, if_exists="replace", index=False)  


csv2sql(engine=engine,
        path = r"C:\\Users\\andy\\Documents\\DE_SQL\\FinalProject\\files\\transactions_01032021.txt",
        table_name='test',
        schema_name="final_proj")

# csv2sql(engine = engine,
#          path=r"C:\\Users\\andy\Documents\\DE_SQL\\FinalProject\\files\\transactions_03032021.txt",
#          table_name='test_tmp',
#          schema_name='final_proj')

# def write_files_to_stg_tables(files):
#     for key in files.keys():
#         for file in files[key]: 
#             file_path = file[1]
#             extension = os.path.splitext(file[1])[1]

#             if extension == '.txt':
#                 csv2sql(engine = engine, path = file_path, table_name = f"STG_{key}", schema_name="final_proj")
#                 copy_file_and_remove(file_path)
#             elif extension == '.xlsx':
#                 xlsx2sql(engine = engine, path = file_path, table_name = f"STG_{key}", schema_name="final_proj")
#                 copy_file_and_remove(file_path)
            

# write_files_to_stg_tables(files)



