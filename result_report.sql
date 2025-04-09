set search_path to final_proj;

with all_tab as (
	select 
		tran.transaction_date,
		cli.*,
		tran.card_num,
		acc.client,
		acc.valid_to
	from dwh_dim_transactions_hist tran
	left join cards card
	on tran.card_num = card.card_num
	left join accounts acc
	on card.account  = acc.account 
	left join clients cli
	on acc.client = cli.client_id
	where tran.transaction_date  > (select min(transaction_date) from stg_transactions)
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
		from dwh_dim_transactions_hist trans
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
			from dwh_dim_transactions_hist 
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


--
---- 1) клиенты с плохим или просроченным пасспортом
--
--select 
--	t1.date as event_dt,
--	t1.passport_num as passport,
--	concat(t1.last_name,' ',t1.first_name,' ',t1.patronymic) as fio,
--	t1.phone as phone,
--	'problem_with_passport' as event_type,
--	current_date as report_dt
--from (
--	select * from clients c
--	left join dwh_dim_passport_blacklist_hist p
--	on c.passport_num = p.passport 
--	where 
--			p.passport is not null 
--	    or c.passport_valid_to is null 
--	    or c.passport_valid_to < p.date
--) t1


