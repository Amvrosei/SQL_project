import pandas as pd
import psycopg2
from sqlalchemy import create_engine, types

conf = {
    "host":"localhost",
    "database":"sdb",
    "user":"postgres",
    "password":"1989",
    "port":"5432"
}

conn = psycopg2.connect(**conf)
cursor = conn.cursor()
cursor.execute("create schema if not exists final_proj")
cursor.execute("set search_path to final_proj")
conn.commit()

engine = create_engine(
    f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}'
    )

#============ create transactions tables
def csv2sql_stg_transactions(engine, path, table_name, schema_name): 
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


def create_dwh_dim_transactions_hist():
    cursor.execute("""
        CREATE TABLE if not exists dwh_dim_transactions_hist(
            id Serial primary key ,
            transaction_id numeric (20) , 
            transaction_date varchar(64) , 
            amount integer ,
            card_num varchar(64) ,
            oper_type varchar(64) , 
            oper_result varchar(64) , 
            terminal varchar(64) ,
            effective_from timestamp default current_timestamp,
            effective_to timestamp default (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')),
            deleted_flg int default 0      
        )       
    """)

    cursor.execute("""
        DROP VIEW if exists v_dwh_dim_transactions_hist
    """)

    # представление актуальных объявлений
    cursor.execute("""
        CREATE VIEW v_dwh_dim_transactions_hist AS 
            SELECT 
                transaction_id , 
                transaction_date ,
                amount ,
                card_num ,
                oper_type , 
                oper_result , 
                terminal 
        FROM dwh_dim_transactions_hist 
        WHERE deleted_flg =  0
        and current_timestamp between effective_from and effective_to 
    """)

    conn.commit()

def create_stg_transactions_new_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_transactions_new_rows as       
            select 
                t1.*
            from final_proj."stg_transactions" t1
            left join v_dwh_dim_transactions_hist t2
            on t1.transaction_id = t2.transaction_id 
            where t2.transaction_id is null  
    """)
    conn.commit()

def create_stg_transactions_deleted_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_transactions_deleted_rows as       
            select 
                t1.*
            from v_dwh_dim_transactions_hist   t1
            left join final_proj."stg_transactions" t2
            on t1.transaction_id = t2.transaction_id 
            where t2.transaction_id is null       
    """)
    conn.commit()

def create_stg_transactions_updated_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_transactions_updated_rows as       
            select 
                t2.*
            from v_dwh_dim_transactions_hist t1
            inner join final_proj."stg_transactions" t2
            on t1.transaction_id = t2.transaction_id 
                and ( 
                   t1.transaction_id <> t2.transaction_id
                or to_timestamp(t1.transaction_date,'YYYY-MM-DD HH24:MI:SS') <> to_timestamp(t2.transaction_date,'YYYY-MM-DD HH24:MI:SS')
                or t1.amount <> t2.amount
                or t1.card_num <> t2.card_num
                or t1.oper_type <> t2.oper_type
                or t1.oper_result <> t2.oper_result
                or t1.terminal <> t2.terminal            
                )
    """)
    conn.commit()

def remove_stg_transactions_tables():
    cursor.execute(""" DROP TABLE if exists final_proj."stg_transactions" """)
    cursor.execute(""" DROP TABLE if exists stg_transactions_new_rows """)
    cursor.execute(""" DROP TABLE if exists stg_transactions_deleted_rows""")
    cursor.execute(""" DROP TABLE if exists stg_transactions_updated_rows """)
    cursor.execute(""" DROP VIEW if exists v_dwh_dim_transactions_hist """)
    conn.commit()

def update_dwh_dim_transactions_hist():
    # добавление новых записей
    cursor.execute("""
        INSERT INTO 
            dwh_dim_transactions_hist(
                transaction_id , 
                transaction_date ,
                amount ,
                card_num ,
                oper_type , 
                oper_result , 
                terminal 
            )
        select 
            transaction_id , 
            transaction_date ,
            amount ,
            card_num ,
            oper_type , 
            oper_result , 
            terminal                  
        from stg_transactions_new_rows
    """)
    
    # у всех измененных полей effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_transactions_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where transaction_id in (select transaction_id from stg_transactions_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # добавляем изменные поля
    cursor.execute("""
        INSERT INTO 
            dwh_dim_transactions_hist (
                transaction_id , 
                transaction_date ,
                amount ,
                card_num ,
                oper_type , 
                oper_result , 
                terminal 
            ) 
        select 
            transaction_id , 
            transaction_date ,
            amount ,
            card_num ,
            oper_type , 
            oper_result , 
            terminal 
        from stg_transactions_updated_rows
    """) 

    # все удаленные поля effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_transactions_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where transaction_id in (select transaction_id from stg_transactions_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # deleted_flg  = 1
    cursor.execute("""
        INSERT INTO dwh_dim_transactions_hist (
            transaction_id , 
            transaction_date ,
            amount ,
            card_num ,
            oper_type , 
            oper_result , 
            terminal ,
            deleted_flg
        )
        select   
            transaction_id , 
            transaction_date ,
            amount ,
            card_num ,
            oper_type , 
            oper_result , 
            terminal , 
            1     
        from stg_transactions_deleted_rows
    """) 

    conn.commit()

def drop_view_transactions():
    cursor.execute("""
            DROP VIEW if exists v_dwh_dim_transactions_hist
        """)
    conn.commit()

# create_dwh_dim_transactions_hist()
# create_stg_transactions_new_rows()
# create_stg_transactions_deleted_rows()
# create_stg_transactions_updated_rows()
# update_dwh_dim_transactions_hist()
# remove_stg_transactions_tables()
# drop_view_transactions()