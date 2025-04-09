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

def create_dwh_dim_antifraud_hist():
    cursor.execute("""
        CREATE TABLE if not exists dwh_dim_antifraud_hist(
            event_dt timestamp,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date,
            create_dt timestamp default current_timestamp,
            update_dt timestamp default (to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS'))       
        );
    """)

    cursor.execute("""
        DROP VIEW if exists v_dwh_dim_antifraud_hist
    """)

    # представление актуальных объявлений
    cursor.execute("""
        CREATE VIEW v_dwh_dim_antifraud_hist AS 
            SELECT 
                event_dt, passport, fio, phone, event_type, report_dt 
        FROM dwh_dim_antifraud_hist
        WHERE current_timestamp between create_dt and update_dt
        or update_dt = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)
    conn.commit()

def stg_antifraud():
    cursor.execute("""
        CREATE TABLE if not exists stg_antifraud (
            event_dt timestamp,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )    
    """)
    conn.commit()

def antifraud_new():
    cursor.execute("""
        with all_tab as (
            select 
                tran.transaction_date,
                cli.*,
                tran.card_num,
                acc.client,
                acc.valid_to
            from final_proj."dwh_dim_transactions_hist" tran
            left join cards card
            on tran.card_num = card.card_num
            left join accounts acc
            on card.account  = acc.account 
            left join clients cli
            on acc.client = cli.client_id
            where tran.transaction_date  > (select min(transaction_date) from final_proj."stg_transactions")
        ),
        task_1 as(
            select
                all_tab.transaction_date as event_dt,
                all_tab.passport_num as passport,
                concat(all_tab.last_name, ' ', all_tab.first_name, ' ', all_tab.patronymic) as fio,
                all_tab.phone as phone,
                'task_1:bad_passport' as event_type,
                current_date as report_dt
            from all_tab 
            where all_tab.passport_num in (select passport from dwh_dim_passport_blacklist_hist   )
            or all_tab.passport_num in (select passport_num 
                                        from all_tab 
                                        where passport_valid_to is not null 
                                        and passport_valid_to < to_date(all_tab.transaction_date,'YYYY-MM-DD')  
                                        )
        ),
        task_2 as (
            select
                all_tab.transaction_date as event_dt,
                all_tab.passport_num as passport,
                concat(all_tab.last_name, ' ', all_tab.first_name, ' ', all_tab.patronymic) as fio,
                all_tab.phone as phone,
                'task_2:bad_account' as event_type,
                current_date as report_dt
            from all_tab 
            where all_tab.valid_to < to_date(all_tab.transaction_date,'YYYY-MM-DD') 
        ),
        task_3 as (
            select
                transaction_date as event_dt,
                passport_num as passport,
                concat(last_name, ' ', first_name, ' ', patronymic) as fio,
                phone as phone,
                'task_3:different_cities' as event_type,
                current_date as report_dt
            from (
                (select 
                    trans.card_num ,
                    count(distinct term.terminal_city)	
                from final_proj."dwh_dim_transactions_hist" trans
                left join dwh_dim_terminals_hist term 
                on trans.terminal = term.terminal_id 
                group by trans.card_num
                having max(to_timestamp(trans.transaction_date, 'YYYY-MM-DD HH24:MI:SS')) - min(to_timestamp(trans.transaction_date, 'YYYY-MM-DD HH24:MI:SS')) < '1 hour'
                and count(distinct term.terminal_city) > 1
                ) t
                left join all_tab alt
                on t.card_num = alt.card_num
            )	
        ),
        task_4 as (
            select
                tmp.transaction_date as event_dt,
                alt.passport_num as passport,
                concat(alt.last_name, ' ', alt.first_name, ' ', alt.patronymic) as fio,
                alt.phone as phone,
                'task_4:fraud_operations' as event_type,
                current_date as report_dt
            from (
                select 
                    t.*
                from (
                    select
                        id,
                        transaction_id,
                        transaction_date,
                        oper_result,
                        oper_type,
                        amount,
                        card_num,
                        terminal,
                        LAG(oper_result, 1) over (partition by card_num order by transaction_date) as previous_result_1,
                        LAG(oper_result, 2) over (partition by card_num order by transaction_date) as previous_result_2,
                        LAG(oper_result, 3) over (partition by card_num order by transaction_date) as previous_result_3,
                        LAG(amount, 1) over (partition by card_num order by transaction_date) as previous_amt_1,
                        LAG(amount, 2) over (partition by card_num order by transaction_date) as previous_amt_2,
                        LAG(amount, 3) over (partition by card_num order by transaction_date) as previous_amt_3,
                        LAG(transaction_date, 3) over (partition by card_num order by transaction_date) as previous_date_3
                    from final_proj."dwh_dim_transactions_hist" 
                ) t
                where
                        oper_result = 'SUCCESS'
                        and previous_result_1 = 'REJECT'
                        and previous_result_2 = 'REJECT'
                        and previous_result_3 = 'REJECT'
                        and previous_amt_3 > previous_amt_2
                        and previous_amt_3 is not Null
                        and previous_amt_2 > previous_amt_1
                        and previous_amt_2 is not Null
                        and previous_amt_1 > amount
                        and previous_amt_1 is not Null
                        and to_timestamp(transaction_date, 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(previous_date_3, 'YYYY-MM-DD HH24:MI:SS') < '20 minutes'
            ) tmp
            left join all_tab alt
            on tmp.card_num = alt.card_num
        ),
        all_frauds as(
        (select * from task_1)
        union all
        (select * from task_2)
        union all
        (select * from task_3)
        union all
        (select * from task_4)
        )

        select 
            min(event_dt) as event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        from all_frauds
        group by fio,
                passport,
                fio,
                phone,
                event_type,
                report_dt

        
    """)
    return [rec for rec in cursor.fetchall()]


def insert_stg_antifraud(array):
    cursor.executemany(                 
        """INSERT INTO stg_antifraud 
                (event_dt, passport, fio, phone, event_type, report_dt) 
            VALUES (%s,%s,%s,%s,%s,%s)""", array
    )
    conn.commit()

def create_stg_antifraud_new_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_antifraud_new_rows as       
            select 
                t1.*
            from final_proj."stg_antifraud" t1
            left join v_dwh_dim_antifraud_hist t2
            on t1.passport = t2.passport
            where t2.passport is null  
    """)
    conn.commit()

# def create_stg_antifraud_deleted_rows():
#     cursor.execute("""
#         CREATE TABLE if not exists stg_antifraud_deleted_rows as       
#             select 
#                 t1.*
#             from v_dwh_dim_antifraud_hist t1
#             left join final_proj."stg_antifraud" t2
#             on t1.passport = t2.passport 
#             where t2.passport is null       
#     """)
#     conn.commit()

def create_stg_antifraud_updated_rows():
    cursor.execute("""
        CREATE TABLE if not exists stg_antifraud_updated_rows as       
            select 
                t2.*
            from v_dwh_dim_antifraud_hist t1
            inner join final_proj."stg_antifraud" t2
            on t1.passport = t2.passport
                and ( 
                   t1.event_dt <> t2.event_dt
                or t1.fio <> t2.fio
                or t1.phone <> t2.phone 
                or t1.event_type <> t2.event_type
                or t1.report_dt <> t2.report_dt             
                )
    """)
    conn.commit()

def update_dwh_dim_antifraud_hist():
    # добавление новых записей
    cursor.execute("""
        INSERT INTO 
            dwh_dim_antifraud_hist 
                   (event_dt, passport, fio, phone, event_type, report_dt )
        select 
            event_dt, passport, fio, phone, event_type, report_dt                  
        from stg_antifraud_new_rows
    """)

    # у всех измененных полей update_dt = date_trunc('second', now() - interval '1 second') 
    cursor.execute("""
        UPDATE dwh_dim_antifraud_hist 
        set update_dt = date_trunc('second', now() - interval '1 second') 
        where passport in (select passport from stg_antifraud_updated_rows)
        and update_dt = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
    """)

    # добавляем изменные поля
    cursor.execute("""
        INSERT INTO dwh_dim_antifraud_hist 
            (event_dt, passport, fio, phone, event_type, report_dt) 
        select 
            event_dt, passport, fio, phone, event_type, report_dt 
        from stg_antifraud_updated_rows
    """) 
    conn.commit()
 
def rep_fraud():
    cursor.execute(""" DROP TABLE IF EXISTS rep_fraud """)
    conn.commit()

    cursor.execute("""           
        CREATE TABLE if not exists rep_fraud AS
            SELECT 
                    event_dt, passport, fio, phone, event_type, report_dt 
            FROM 
                dwh_dim_antifraud_hist
            WHERE 
                update_dt = to_timestamp('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
                --current_timestamp between date_trunc('second', create_dt - interval '1 second') and update_dt
                --or          
    """)
    conn.commit()

def remove_stg_antifraud_tables():
    cursor.execute(""" DROP TABLE if exists stg_antifraud """)
    cursor.execute(""" DROP TABLE if exists stg_antifraud_new_rows """)
    #cursor.execute(""" DROP TABLE if exists stg_antifraud_deleted_rows """)
    cursor.execute(""" DROP TABLE if exists stg_antifraud_updated_rows """)
    cursor.execute(""" DROP VIEW if exists v_dwh_dim_antifraud_hist """)
    conn.commit()

# stg_antifraud()
# new_rows = antifraud_new()
# print(new_rows)
# insert_stg_antifraud(new_rows)

# create_dwh_dim_antifraud_hist()
# create_stg_antifraud_new_rows()

# create_stg_antifraud_updated_rows()
# update_dwh_dim_antifraud_hist()
# remove_stg_antifraud_tables()
# rep_fraud()



 