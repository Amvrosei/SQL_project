import psycopg2

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

#============ create terminals tables
def create_dwh_dim_terminals_hist():
    cursor.execute("""
        CREATE TABLE if not exists dwh_dim_terminals_hist(
            id Serial primary key,
            terminal_id varchar(128), 
            terminal_type varchar(128),
            terminal_city varchar(128), 
            terminal_address varchar(128),            
            effective_from timestamp default current_timestamp,
            effective_to timestamp default (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')),
            deleted_flg int default 0      
        )       
    """)

    cursor.execute("""
        DROP VIEW if exists v_dwh_dim_terminals_hist
    """)

    # представление актуальных объявлений
    cursor.execute("""
        CREATE VIEW v_dwh_dim_terminals_hist AS 
            SELECT 
                   terminal_id, terminal_type, terminal_city, terminal_address 
        FROM dwh_dim_terminals_hist 
        WHERE deleted_flg =  0
        and current_timestamp between effective_from and effective_to 
        -- and effective_to = (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'))
    """)

    conn.commit()

def create_stg_terminals_new_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_terminals_new_rows as       
            select 
                t1.*
            from final_proj."stg_terminals" t1
            left join v_dwh_dim_terminals_hist t2
            on t1.terminal_id = t2.terminal_id 
            where t2.terminal_id is null  
    """)
    conn.commit()

def create_stg_terminals_deleted_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_terminals_deleted_rows as       
            select 
                t1.*
            from v_dwh_dim_terminals_hist   t1
            left join final_proj."stg_terminals" t2
            on t1.terminal_id = t2.terminal_id 
            where t2.terminal_id is null       
    """)
    conn.commit()

def create_stg_terminals_updated_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_terminals_updated_rows as       
            select 
                t2.*
            from v_dwh_dim_terminals_hist t1
            inner join final_proj."stg_terminals" t2
            on t1.terminal_id = t2.terminal_id 
                and ( 
                   t1.terminal_type <> t2.terminal_type 
                or t1.terminal_city <> t2.terminal_city 
                or t1.terminal_address <> t2.terminal_address                
                )
    """)
    conn.commit()


def remove_stg_terminals_tables():
    cursor.execute(""" DROP TABLE if exists final_proj."stg_terminals" """)
    cursor.execute(""" DROP TABLE if exists stg_terminals_new_rows """)
    cursor.execute(""" DROP TABLE if exists stg_terminals_deleted_rows""")
    cursor.execute(""" DROP TABLE if exists stg_terminals_updated_rows """)
    cursor.execute(""" DROP VIEW if exists v_dwh_dim_terminals_hist """)
    conn.commit()


def update_dwh_dim_terminals_hist():
    # добавление новых записей
    cursor.execute("""
        INSERT INTO 
            dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address)
        select 
            terminal_id, terminal_type, terminal_city, terminal_address                  
        from stg_terminals_new_rows
    """)
    
    # у всех измененных полей effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_terminals_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where terminal_id in (select terminal_id from stg_terminals_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # добавляем изменные поля
    cursor.execute("""
        INSERT INTO 
            dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address) 
        select 
            terminal_id, terminal_type, terminal_city, terminal_address 
        from stg_terminals_updated_rows
    """) 

    # все удаленные поля effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_terminals_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where terminal_id in (select terminal_id from stg_terminals_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # deleted_flg  = 1
    cursor.execute("""
        INSERT INTO dwh_dim_terminals_hist (
                   terminal_id, terminal_type,
                   terminal_city, terminal_address, deleted_flg)
        select   
            terminal_id, terminal_type, terminal_city, terminal_address, 1     
        from stg_terminals_deleted_rows
    """) 

    conn.commit()

def drop_view_terminals():
    cursor.execute("""
            DROP VIEW if exists v_dwh_dim_terminals_hist
        """)
    conn.commit()

# create_dwh_dim_terminals_hist()
# create_stg_terminals_new_rows()
# create_stg_terminals_deleted_rows()
# create_stg_terminals_updated_rows()
# update_dwh_dim_terminals_hist()
# remove_stg_terminals_tables()
# drop_view_terminals()