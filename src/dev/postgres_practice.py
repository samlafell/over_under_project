
# Working with PostgreSQL
# Installing in Python: https://stackoverflow.com/questions/33866695/error-installing-psycopg2-on-macos-10-9-5
import psycopg2

# * Create the connection
# Had to create this admin account via terminal
# https://www.sqlshack.com/setting-up-a-postgresql-database-on-mac/
connection = psycopg2.connect(database="postgres", user="admin", password="password", host="127.0.0.1", port="5432")

# Create Database
def create_database(connection, database_name):
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {database_name}")
        
# Drop Database
def drop_database(connection, database_name):
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"DROP DATABASE {database_name}")

# Check to see if table exists
def table_exists(con, table_str):
    exists = False
    try:
        with con.cursor() as cursor:
            cursor.execute(f"select exists(select relname from pg_class where relname='{table_str}')")
            exists = cursor.fetchone()[0]
    except psycopg2.Error as e:
        print(e)
    return exists


# Check Col Names in a table
def get_table_col_names(con, table_str):
    col_names = []
    try:
        with con.cursor() as cursor:
            cursor.execute(f"select * from {table_str} LIMIT 0")
            for desc in cursor.description:
                col_names.append(desc[0])
    except psycopg2.Error as e:
        print(e)

    return col_names


# Create Test
create_database(connection, 'test')

# Drop Test
drop_database(connection, 'test')

# Test for table
table_name = 'atl.batters_2017'
if table_exists(connection, table_name):
    col_names = get_table_col_names(connection, table_name)
    print(col_names)
else:
    print('table does not exist')


# Close the connection
connection.close()
        
    