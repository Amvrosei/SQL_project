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

#============ create passport_blacklist tables
def create_dwh_dim_passport_blacklist_hist():
    cursor.execute("""
        CREATE TABLE if not exists dwh_dim_passport_blacklist_hist(
            id Serial primary key,
            date timestamp,	
            passport varchar(128),       
            effective_from timestamp default current_timestamp,
            effective_to timestamp default (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')),
            deleted_flg int default 0      
        )       
    """)

    cursor.execute("""
        DROP VIEW if exists v_dwh_dim_passport_blacklist_hist
    """)

    # представление актуальных объявлений
    cursor.execute("""
        CREATE VIEW v_dwh_dim_passport_blacklist_hist AS 
            SELECT 
                   date, passport
        FROM dwh_dim_passport_blacklist_hist 
        WHERE deleted_flg =  0
        and current_timestamp between effective_from and effective_to 
        -- and effective_to = (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'))
    """)

    conn.commit()


def create_stg_passport_blacklist_new_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_passport_blacklist_new_rows as       
            select 
                t1.*
            from final_proj."stg_passport_blacklist" t1
            left join v_dwh_dim_passport_blacklist_hist t2
            on t1.passport = t2.passport 
            where t2.passport is null  
    """)
    conn.commit()

def create_stg_passport_blacklist_deleted_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_passport_blacklist_deleted_rows as       
            select 
                t1.*
            from v_dwh_dim_passport_blacklist_hist t1
            left join final_proj."stg_passport_blacklist" t2
            on t1.passport = t2.passport 
            where t2.passport is null       
    """)
    conn.commit()

def create_stg_passport_blacklist_updated_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_passport_blacklist_updated_rows as       
            select 
                t2.*
            from v_dwh_dim_passport_blacklist_hist t1
            inner join final_proj."stg_passport_blacklist" t2
            on t1.passport = t2.passport 
                and (t1.date <> t2.date)
    """)
    conn.commit()


def remove_stg_passport_blacklist_tables():
    cursor.execute(""" DROP TABLE if exists final_proj."stg_passport_blacklist" """)
    cursor.execute(""" DROP TABLE if exists stg_passport_blacklist_new_rows """)
    cursor.execute(""" DROP TABLE if exists stg_passport_blacklist_deleted_rows""")
    cursor.execute(""" DROP TABLE if exists stg_passport_blacklist_updated_rows """)
    cursor.execute(""" DROP VIEW if exists v_dwh_dim_passport_blacklist_hist """)
    conn.commit()


def update_dwh_dim_passport_blacklist_hist():
    # добавление новых записей
    cursor.execute("""
        INSERT INTO 
            dwh_dim_passport_blacklist_hist (date, passport)
        select 
            date, passport                  
        from stg_passport_blacklist_new_rows
    """)
    
    # у всех измененных полей effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_passport_blacklist_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where passport in (select passport from stg_passport_blacklist_updated_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # добавляем изменные поля
    cursor.execute("""
        INSERT INTO 
            dwh_dim_passport_blacklist_hist (date, passport) 
        select 
            date, passport 
        from stg_passport_blacklist_updated_rows
    """) 

    # все удаленные поля effective_to = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_passport_blacklist_hist
        set effective_to = date_trunc('second', now() - interval '1 second') 
        where passport in (select passport from stg_passport_blacklist_deleted_rows)
        and effective_to = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # deleted_flg  = 1
    cursor.execute("""
        INSERT INTO dwh_dim_passport_blacklist_hist (date, passport, deleted_flg)
        select   
            date, passport, 1     
        from stg_passport_blacklist_deleted_rows
    """) 

    conn.commit()

def drop_view_passport_blacklist():
    cursor.execute("""
            DROP VIEW if exists v_dwh_dim_passport_blacklist_hist
        """)
    conn.commit()

# create_DWH_DIM_passport_blacklist_hist()
# create_stg_passport_blacklist_new_rows()
# create_stg_passport_blacklist_deleted_rows()
# create_stg_passport_blacklist_updated_rows()
# update_DWH_DIM_passport_blacklist_HIST()
# remove_stg_passport_blacklist_tables()
# drop_view_passport_blacklist()