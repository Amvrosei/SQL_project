import psycopg2
from psycopg2 import sql

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

conn.commit()

try:
    # Connect to your postgres DB
    conn = psycopg2.connect(**conf)

    # Open a cursor to perform database operations
    cursor = conn.cursor()
    cursor.execute("set search_path to final_proj")

    table_name = r"STG_terminals"
    # Execute a query to drop the table
    cursor.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(table_name)))

    # Commit changes
    conn.commit()
    #cur.close()
    #conn.close()

    print(f"Table '{table_name}' dropped successfully.")

except Exception as e:
    print(f"An error occurred: {e}")


# def remove_tmp_tables():
#     cursor.execute("set search_path to final_proj")
#     #cursor.execute(""" DROP TABLE if exists final_proj."STG_terminals" """)
#     try:
#         #cursor.execute('DROP TABLE final_proj.STG_terminals ')
#         cursor.execute(""" DROP TABLE tmp_terminals_new_rows """ )
#         #cursor.execute(""" DROP TABLE if exists tmp_terminals_deleted_rows1 """)
#         #cursor.execute(""" DROP TABLE if exists tmp_terminals_updated_rows """)
#     except Exception as ex:
#         print(ex)
#     else:
#         print('ok')

# print('try removed table')
# remove_tmp_tables()