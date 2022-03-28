#!/usr/bin/python

import pandas as pd
import jaydebeapi
import glob
import os


conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:de2tm/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',['de2tm', 'balinfundinson'],'/home/de2tm/ojdbc8.jar')

curs=conn.cursor()


#ОТКЛЮЧАЕМ АВТОМАТИЧЕСКОЕ СОЗДАНИЕ ТОЧКИ ФИКСАЦИИ 
conn.jconn.setAutoCommit(False)


#ОБЪЯВЛЕНИЕ ПУТИ К ФАЙЛАМ

passport_blacklist_glob = glob.glob('/home/de2tm/ykuz/passport_blacklist*.xlsx')

terminals_glob = glob.glob('/home/de2tm/ykuz/terminals*.xlsx')

transactions_glob = glob.glob('/home/de2tm/ykuz/transactions*.csv')

archive_glob=glob.glob('/home/de2tm/ykuz/archive')


#ФОРМИРОВАНИЕ DATAFRAME

terminals_df=pd.read_excel(terminals_glob[0],sheet_name='terminals',index_col=None)

terminal_id_df=pd.read_excel(terminals_glob[0],sheet_name='terminals',index_col=None,usecols='A')

passport_blacklist_df=pd.read_excel(passport_blacklist_glob[0],index_col=None,sep=',',converters={'date':str})

transactions_df=pd.read_csv(transactions_glob[0],sep=';', encoding='latin-1')

#ПОЛУЧЕНИЕ ДАТЫ ДЛЯ TERMINALS*.XLSX
terminals_dt=terminals_glob[0].replace('/home/de2tm/ykuz','')
terminals_dt_1=terminals_dt[11:19]


#ДОБАВЛЯЕМ ДАТУ ИЗ НАЗВАНИЯ TERMINALS*.XLSX В ДАТАФРЕЙМ
terminals_df['create_dt']=terminals_dt_1



#ИНКРЕМЕНТЫ

#очистка стейджинга
curs.execute('DELETE FROM de2tm.ykuz_stg_clnts')
curs.execute('DELETE FROM de2tm.ykuz_stg_accnts')
curs.execute('DELETE FROM de2tm.ykuz_stg_cards')
curs.execute('DELETE FROM de2tm.ykuz_stg_trmnls')

curs.execute('DELETE FROM de2tm.ykuz_stg_delete_clnts')
curs.execute('DELETE FROM de2tm.ykuz_stg_delete_accnts')
curs.execute('DELETE FROM de2tm.ykuz_stg_delete_cards')
curs.execute('DELETE FROM de2tm.ykuz_stg_delete_trmnls')


#ВСТАВКА В СТЕЙДЖИНГ ИЗМЕНЕНИЙ ИСТОЧНИКА

#ykuz_stg_clnts
curs.execute("""
    INSERT INTO de2tm.ykuz_stg_clnts 
        (client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        effective_from_dt, 
        effective_to_dt)
        
        SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            create_dt, 
            TO_DATE('31-12-5999','dd-mm-yyyy')
        FROM BANK.clients
        WHERE COALESCE (update_dt, create_dt) >
            (SELECT last_update_dt FROM de2tm.ykuz_meta_clnts)""")

#ykuz_stg_accnts        
curs.execute("""		
    INSERT INTO de2tm.ykuz_stg_accnts
        (
        account_num,
        valid_to,
        client,
        effective_from_dt, 
        effective_to_dt
        )
        SELECT
            account,
            valid_to,
            client,
            create_dt, 
            TO_DATE('31-12-5999','dd-mm-yyyy')
        FROM BANK.accounts
        WHERE COALESCE (update_dt, create_dt) >
            (SELECT last_update_dt FROM de2tm.ykuz_meta_accnts)""")

#ykuz_stg_cards        
curs.execute("""	
    INSERT INTO de2tm.ykuz_stg_cards
        (
        card_num,
        account_num,
        effective_from_dt, 
        effective_to_dt
        )
        SELECT
            TRIM(card_num),
            account,
            create_dt, 
            TO_DATE('31-12-5999','dd-mm-yyyy')
        FROM BANK.cards
        WHERE COALESCE (update_dt, create_dt) >
            (SELECT last_update_dt FROM de2tm.ykuz_meta_cards)""")
        
#ykuz_stg_trmnls
curs.executemany("""      
    INSERT INTO de2tm.ykuz_stg_trmnls
        (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from_dt, 
        effective_to_dt
        )
        VALUES
            (
            ?,
            ?,
            ?,
            ?,
            TO_DATE(?,'ddmmyyyy'), 
            TO_DATE('31-12-5999','dd-mm-yyyy')
            )""",terminals_df.values.tolist())
        
 
#STG ДЛЯ УДАЛЕНИЯ СТРОК ИЗ ПРИЕМНИКА

#ykuz_stg_delete_clnts
curs.execute("""
    INSERT INTO de2tm.ykuz_stg_delete_clnts (client_id)
        SELECT
            client_id
        FROM BANK.clients""")

#ykuz_stg_delete_accnts	
curs.execute("""
    INSERT INTO de2tm.ykuz_stg_delete_accnts (account_num)
        SELECT
            account
        FROM BANK.accounts""")

#ykuz_stg_delete_cards	
curs.execute("""
    INSERT INTO de2tm.ykuz_stg_delete_cards (card_num)
        SELECT
            card_num
        FROM BANK.cards""")

#ykuz_stg_delete_trmnls
curs.executemany("""  	
INSERT INTO de2tm.ykuz_stg_delete_trmnls 
    (terminal_id)
	VALUES(?)""",terminal_id_df.values.tolist())


    
#INSERT И MERGE СТРЕЙДЖИНГА В ПРИЕМНИК

#INSERT de2tm.ykuz_dwh_dim_clnts_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_clnts_hist
        (
        client_id, 
        last_name, 
        first_name, 
        patronymic, 
        date_of_birth,        
        passport_num, 
        passport_valid_to, 
        phone, 
        effective_from_dt, 
        effective_to_dt, 
        dlt_flg
        )
            SELECT
                client_id,
                last_name,
                first_name,
                patronymic,
                date_of_birth,
                passport_num,
                passport_valid_to,
                phone,
                effective_from_dt, 
                effective_to_dt, 
                'N'
            FROM de2tm.ykuz_stg_clnts
            WHERE effective_to_dt = TO_DATE('31-12-5999','dd-mm-yyyy')
            """)

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_clnts_hist
        (
        client_id, 
        last_name, 
        first_name, 
        patronymic, 
        date_of_birth,        
        passport_num, 
        passport_valid_to, 
        phone, 
        effective_from_dt, 
        effective_to_dt, 
        dlt_flg
        )
            SELECT
                client_id,
                last_name,
                first_name,
                patronymic,
                date_of_birth,
                passport_num,
                passport_valid_to,
                phone,
                effective_from_dt, 
                effective_to_dt, 
                'N'
            FROM de2tm.ykuz_stg_clnts
            WHERE effective_to_dt != TO_DATE('31-12-5999','dd-mm-yyyy')
            """)

#MERGE de2tm.ykuz_dwh_dim_clnts_hist
curs.execute("""
    MERGE INTO de2tm.ykuz_dwh_dim_clnts_hist tgt
    USING de2tm.ykuz_stg_clnts stg
    ON (tgt.client_id=stg.client_id )
    WHEN matched THEN UPDATE SET
        tgt.effective_to_dt=stg.effective_from_dt-1
        WHERE 
        tgt.effective_to_dt = to_date( '5999-12-31', 'YYYY-MM-DD' )
        AND tgt.effective_from_dt < COALESCE(stg.effective_to_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD'))
        """)
        
        
#INSERT de2tm.ykuz_dwh_dim_accnts_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_accnts_hist
        (
        account_num,
        valid_to,
        client,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            account_num,
            valid_to,
            client,
            effective_from_dt,
            effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_accnts
        WHERE effective_to_dt = TO_DATE('31-12-5999','dd-mm-yyyy')
        """)

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_accnts_hist
        (
        account_num,
        valid_to,
        client,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            account_num,
            valid_to,
            client,
            effective_from_dt,
            effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_accnts
        WHERE effective_to_dt != TO_DATE('31-12-5999','dd-mm-yyyy')
        """)

#MERGE de2tm.ykuz_dwh_dim_accnts_hist       
curs.execute("""
    MERGE INTO de2tm.ykuz_dwh_dim_accnts_hist tgt
    USING de2tm.ykuz_stg_accnts stg
    ON (tgt.account_num=stg.account_num )
    WHEN matched THEN UPDATE SET
        tgt.valid_to=stg.valid_to,
        tgt.client=stg.client,
        tgt.effective_from_dt=stg.effective_from_dt, 
        tgt.effective_to_dt=stg.effective_from_dt-1
    WHERE
        tgt.effective_to_dt = to_date( '5999-12-31', 'YYYY-MM-DD' )
        AND tgt.effective_from_dt < COALESCE(stg.effective_to_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD'))
        """)


#INSERT de2tm.ykuz_dwh_dim_cards_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_cards_hist
        (
        card_num,
        account_num,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            card_num,
            account_num,
            effective_from_dt,
            effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_cards
        WHERE effective_to_dt = TO_DATE('31-12-5999','dd-mm-yyyy')
        """)

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_cards_hist
        (
        card_num,
        account_num,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            card_num,
            account_num,
            effective_from_dt,
            effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_cards
        WHERE effective_to_dt != TO_DATE('31-12-5999','dd-mm-yyyy')
        """)

#MERGE de2tm.ykuz_dwh_dim_cards_hist   	
curs.execute("""
    MERGE INTO de2tm.ykuz_dwh_dim_cards_hist tgt
    USING de2tm.ykuz_stg_cards stg
    ON (tgt.card_num=stg.card_num)
    WHEN matched THEN UPDATE SET
        tgt.account_num=stg.account_num,
        tgt.effective_from_dt=stg.effective_from_dt, 
        tgt.effective_to_dt=stg.effective_from_dt-1
    WHERE
        tgt.effective_to_dt = to_date( '5999-12-31', 'YYYY-MM-DD' )
        AND tgt.effective_from_dt < COALESCE(stg.effective_to_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD'))
        """)


#INSERT de2tm.ykuz_dwh_dim_trmnls_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_trmnls_hist
        (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city,
            stg.terminal_address,
            stg.effective_from_dt, 
            stg.effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_trmnls stg
            LEFT JOIN de2tm.ykuz_dwh_dim_trmnls_hist tgt
                ON tgt.terminal_id=stg.terminal_id
        WHERE tgt.terminal_id IS NULL
        """)

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_trmnls_hist
        (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city,
            stg.terminal_address,
            stg.effective_from_dt, 
            stg.effective_to_dt,
            'N'
        FROM de2tm.ykuz_stg_trmnls stg
            LEFT JOIN de2tm.ykuz_dwh_dim_trmnls_hist tgt
                ON tgt.terminal_id=stg.terminal_id
            WHERE stg.terminal_city!=tgt.terminal_city
                OR stg.terminal_address!= tgt.terminal_address
                AND tgt.effective_to_dt = TO_DATE('31-12-5999','dd-mm-yyyy')
        """)


#MERGE de2tm.ykuz_dwh_dim_trmnls_hist
   		
curs.execute("""
    MERGE INTO de2tm.ykuz_dwh_dim_trmnls_hist tgt
    USING de2tm.ykuz_stg_trmnls stg
    ON (tgt.terminal_id=stg.terminal_id )
    WHEN matched THEN UPDATE SET
        tgt.effective_to_dt=stg.effective_from_dt-1
    WHERE tgt.effective_to_dt = to_date( '5999-12-31', 'YYYY-MM-DD' )
    AND tgt.effective_from_dt < COALESCE(stg.effective_to_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD'))
        """)



#INSERT для таблиц-фактов

curs.execute("""alter session set NLS_NUMERIC_CHARACTERS = '.,'""")

curs.executemany("""
    INSERT INTO de2tm.ykuz_dwh_fact_trnsctns 
        (
        tran_id,
        trans_date,
        amt,
        card_num,
        oper_type,
        oper_result,
        terminal
        )
         values (?, to_timestamp(?,'yyyy-mm-dd  hh24:mi:ss'), TO_NUMBER(?,'9999999999D99','nls_numeric_characters='',.'''), ?, ?, ?, ?)
    """,transactions_df.values.tolist())



curs.executemany("""
    INSERT INTO de2tm.ykuz_dwh_fact_pssprt_blcklst 
        (
        entry_dt,
        passport_num)
        VALUES (TO_DATE(?,'yyyy-mm-dd hh24:mi:ss'),?)
    """,passport_blacklist_df.values.tolist())




# ОБРАБОТКА ДАННЫХ НА СЛУЧАЙ ИХ УДАЛЕНИЯ

#DELETE FROM de2tm.ykuz_dwh_dim_clnts_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_clnts_hist
        ( 
        client_id, 
        last_name, 
        first_name, 
        patronymic, 
        date_of_birth,        
        passport_num, 
        passport_valid_to, 
        phone, 
        effective_from_dt, 
        effective_to_dt, 
        dlt_flg
        )
        SELECT
            tgt.client_id, 
            tgt.last_name, 
            tgt.first_name, 
            tgt.patronymic, 
            tgt.date_of_birth,        
            tgt.passport_num, 
            tgt.passport_valid_to, 
            tgt.phone, 
            TO_DATE('{}', 'ddmmyyyy'),
            TO_DATE('5999-12-31', 'YYYY-MM-DD'),
            'Y'
        FROM 
            de2tm.ykuz_dwh_dim_clnts_hist tgt
        LEFT JOIN de2tm.ykuz_stg_clnts stg
            ON tgt.client_id = stg.client_id
        WHERE stg.client_id IS NULL
            AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            AND tgt.dlt_flg = 'N'
""".format(terminals_dt_1))

curs.execute("""
    UPDATE de2tm.ykuz_dwh_dim_clnts_hist
    SET
        effective_to_dt = TO_DATE('{}', 'ddmmyyyy')
    WHERE client_id IN
            (
            SELECT tgt.client_id
            FROM  de2tm.ykuz_dwh_dim_clnts_hist tgt
            LEFT JOIN de2tm.ykuz_stg_clnts stg
                ON tgt.client_id = stg.client_id
            WHERE stg.client_id IS NULL
                AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND tgt.dlt_flg = 'N'
            )
        AND effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
        AND dlt_flg = 'N'
""".format(terminals_dt_1))


#DELETE FROM de2tm.ykuz_dwh_dim_accnts_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_accnts_hist
        (
        account_num,
        valid_to,
        client,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            tgt.account_num, 
            tgt.valid_to, 
            tgt.client, 
            TO_DATE('{}', 'ddmmyyyy'),
            TO_DATE('5999-12-31', 'YYYY-MM-DD'),
            'Y'
        FROM 
            de2tm.ykuz_dwh_dim_accnts_hist tgt
        LEFT JOIN de2tm.ykuz_stg_accnts stg
            ON tgt.account_num = stg.account_num
        WHERE stg.account_num IS NULL
            AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            AND tgt.dlt_flg = 'N'
""".format(terminals_dt_1))

curs.execute("""
    UPDATE de2tm.ykuz_dwh_dim_accnts_hist
    SET
        effective_to_dt = TO_DATE('{}', 'ddmmyyyy')
    WHERE account_num IN
            (
            SELECT tgt.account_num
            FROM  de2tm.ykuz_dwh_dim_accnts_hist tgt
            LEFT JOIN de2tm.ykuz_stg_accnts stg
                ON tgt.account_num = stg.account_num
            WHERE stg.account_num IS NULL
                AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND tgt.dlt_flg = 'N'
            )
        AND effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
        AND dlt_flg = 'N'
""".format(terminals_dt_1))


#DELETE FROM de2tm.ykuz_dwh_dim_cards_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_cards_hist
        (
        card_num,
        account_num,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            tgt.card_num, 
            tgt.account_num,
            TO_DATE('{}', 'ddmmyyyy'),
            TO_DATE('5999-12-31', 'YYYY-MM-DD'),
            'Y'
        FROM 
            de2tm.ykuz_dwh_dim_cards_hist tgt
        LEFT JOIN de2tm.ykuz_stg_cards stg
            ON tgt.card_num = stg.card_num
        WHERE stg.card_num IS NULL
            AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            AND tgt.dlt_flg = 'N'
""".format(terminals_dt_1))

curs.execute("""
    UPDATE de2tm.ykuz_dwh_dim_cards_hist
    SET
        effective_to_dt = TO_DATE('{}', 'ddmmyyyy')
    WHERE card_num IN
            (
            SELECT tgt.card_num
            FROM  de2tm.ykuz_dwh_dim_cards_hist tgt
            LEFT JOIN de2tm.ykuz_stg_cards stg
                ON tgt.card_num = stg.card_num
            WHERE stg.card_num IS NULL
                AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND tgt.dlt_flg = 'N'
            )
        AND effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
        AND dlt_flg = 'N'
""".format(terminals_dt_1))


#DELETE FROM de2tm.ykuz_dwh_dim_trmnls_hist

curs.execute("""
    INSERT INTO de2tm.ykuz_dwh_dim_trmnls_hist
        (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from_dt, 
        effective_to_dt,
        dlt_flg
        )
        SELECT
            tgt.terminal_id, 
            tgt.terminal_type,
            tgt.terminal_city,
            tgt.terminal_address,
            TO_DATE('{}', 'ddmmyyyy'),
            TO_DATE('5999-12-31', 'YYYY-MM-DD'),
            'Y'
        FROM 
            de2tm.ykuz_dwh_dim_trmnls_hist tgt
        LEFT JOIN de2tm.ykuz_stg_trmnls stg
            ON tgt.terminal_id = stg.terminal_id
        WHERE stg.terminal_id IS NULL
            AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            AND tgt.dlt_flg = 'N'
""".format(terminals_dt_1))

curs.execute("""
    UPDATE de2tm.ykuz_dwh_dim_trmnls_hist
    SET
        effective_to_dt = TO_DATE('{}', 'ddmmyyyy')
    WHERE terminal_id IN
            (
            SELECT tgt.terminal_id
            FROM  de2tm.ykuz_dwh_dim_trmnls_hist tgt
            LEFT JOIN de2tm.ykuz_stg_trmnls stg
                ON tgt.terminal_id = stg.terminal_id
            WHERE stg.terminal_id IS NULL
                AND tgt.effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                AND tgt.dlt_flg = 'N'
            )
        AND effective_to_dt = TO_DATE('5999-12-31', 'YYYY-MM-DD')
        AND dlt_flg = 'N'
""".format(terminals_dt_1))



#ОБНОВЛЕНИЕ МЕТА-ДАННЫХ

#UPDATE  de2tm.ykuz_meta_clnts 
curs.execute("""
    UPDATE  de2tm.ykuz_meta_clnts 
        SET last_update_dt = (
            SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) 
            FROM de2tm.ykuz_dwh_dim_clnts_hist)
        WHERE (SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) FROM de2tm.ykuz_dwh_dim_clnts_hist) IS NOT NULL
""")

#UPDATE  de2tm.ykuz_meta_accnts
curs.execute("""
    UPDATE  de2tm.ykuz_meta_accnts
        SET last_update_dt = (
            SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) 
            FROM de2tm.ykuz_dwh_dim_accnts_hist)
    WHERE (SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) FROM de2tm.ykuz_dwh_dim_accnts_hist) IS NOT NULL
""")

#UPDATE  de2tm.ykuz_meta_cards
curs.execute("""
    UPDATE  de2tm.ykuz_meta_cards
        SET last_update_dt = (
            SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) 
            FROM de2tm.ykuz_dwh_dim_cards_hist)
    WHERE (SELECT MAX(COALESCE (effective_from_dt,effective_to_dt))FROM de2tm.ykuz_dwh_dim_cards_hist) IS NOT NULL
""")

#UPDATE  de2tm.ykuz_meta_trmnls
curs.execute("""
    UPDATE  de2tm.ykuz_meta_trmnls
        SET last_update_dt = (
            SELECT MAX(COALESCE (effective_from_dt,effective_to_dt))
            FROM de2tm.ykuz_dwh_dim_trmnls_hist)
    WHERE (SELECT MAX(COALESCE (effective_from_dt,effective_to_dt)) FROM de2tm.ykuz_dwh_dim_trmnls_hist) IS NOT NULL
""")
	
    
    

#ТАБЛИЦА ОТЧЁТА

curs.execute("""
INSERT INTO de2tm.ykuz_rep_fraud 
	(event_dt, passport, fio, phone, event_type, report_dt)
WITH event_data AS 
	(
	SELECT
		trans_date,
		passport_num,
		last_name||' '||first_name||' '||patronymic,
		phone,
		CASE 
			WHEN psbl_passport_num IS NOT NULL  
				OR passport_valid_to<TO_DATE('{0}', 'ddmmyyyy')
			THEN 'Совершение операции при просроченном или заблокированном паспорте'
			
			WHEN valid_to IS NULL 
				OR valid_to<TO_DATE('{1}', 'ddmmyyyy')
			THEN 'Совершение операции при недействующем договоре'
			
			WHEN next_terminal_city IS NOT NULL
				AND terminal_city!= next_terminal_city 
				AND (next_trans_date-trans_date)<INTERVAL '1' HOUR
			THEN 'Совершение операций в разных городах в течение одного часа'
			
			WHEN oper_result='SUCCESS'
				AND prev_oper_result='REJECT'
				AND before_prev_result='REJECT'
				AND prev_amt BETWEEN amt AND before_prev_amt
				AND (trans_date-before_prev_date) < INTERVAL '20' MINUTE
			THEN 'Попытка подборка суммы'
		END event_type_case,
		CAST(TO_TIMESTAMP(trans_date) as DATE)+INTERVAL '1' DAY
	FROM 
		(
		
		SELECT 
			trn.tran_id,trn.trans_date,trn.amt,trn.oper_result,
            trn.card_num,
            
            term.terminal_id,
            term.terminal_city, 
            acc.account_num,
            acc.valid_to,
            cl.client_id,
            cl.last_name,
            cl.first_name,
            cl.patronymic,
            cl.passport_num,
            cl.passport_valid_to,
            cl.phone,
            psbl.entry_dt as psbl_entry_dt,
            psbl.passport_num as psbl_passport_num, 
			
            LEAD (term.terminal_city) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS next_terminal_city,
            
			LEAD (trn.trans_date) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS next_trans_date,
			
            LAG (trn.trans_date,2) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS before_prev_date,
			
            LAG (trn.oper_result) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS prev_oper_result,
			
            LAG (trn.oper_result,2) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS before_prev_result,
			
            LAG (trn.amt) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS prev_amt,
			
            LAG (trn.amt,2) OVER (PARTITION BY cl.client_id ORDER BY trn.trans_date) AS before_prev_amt
		FROM
            de2tm.ykuz_dwh_fact_trnsctns trn
    
            LEFT JOIN de2tm.ykuz_dwh_dim_cards_hist crds
                ON trim(crds.card_num)=trim(trn.card_num)
            
            LEFT JOIN de2tm.ykuz_dwh_dim_trmnls_hist term
                ON term.terminal_id=trn.terminal
            
            LEFT JOIN de2tm.ykuz_dwh_dim_accnts_hist acc
                ON crds.account_num=acc.account_num
                
            LEFT JOIN de2tm.ykuz_dwh_dim_clnts_hist cl
                ON cl.client_id=acc.client
                
            LEFT JOIN de2tm.ykuz_dwh_fact_pssprt_blcklst psbl
                ON psbl.passport_num=cl.passport_num
		)
	)
SELECT * FROM event_data WHERE event_type_case IS NOT NULL
""".format(terminals_dt_1,terminals_dt_1))


#ЗАКИДЫВАНИЕ ОТЧЁТА В ФАЙЛ

ykuz_rep_fraud_df=pd.read_sql("""SELECT * FROM de2tm.ykuz_rep_fraud""",conn)

ykuz_rep_fraud_df.to_excel('/home/de2tm/ykuz/ykuz_rep_fraud.xlsx', sheet_name ='report', header=True,index=False)

#ПЕРЕМЕЩАЕМ ИСПОЛЬЗОВАННЫЕ ФАЙЛЫ В ПАПКУ /home/de2tm/ykuz/archive

trans_arch=transactions_glob[0].replace('/home/de2tm/ykuz','/home/de2tm/ykuz/archive')
term_arch=terminals_glob[0].replace('/home/de2tm/ykuz','/home/de2tm/ykuz/archive')
passport_bl_arch=passport_blacklist_glob[0].replace('/home/de2tm/ykuz','/home/de2tm/ykuz/archive')

os.replace(transactions_glob[0],trans_arch)
os.replace(terminals_glob[0],term_arch)
os.replace(passport_blacklist_glob[0],passport_bl_arch)

os.rename(trans_arch,trans_arch+'.backup')
os.rename(term_arch,term_arch+'.backup')
os.rename(passport_bl_arch,passport_bl_arch+'.backup')


#СОЗДАНИЕ ТОЧКИ ФИКСАЦИИ
conn.commit()

#ЗАКРЫВАЕМ СОЕДИНЕНИЕ
conn.close()










