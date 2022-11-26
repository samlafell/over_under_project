import psycopg
from psycopg import sql
import pandas as pd

# Create Database
def create_database(connection, database_name):
    connection.autocommit = True
    with connection.cursor() as cursor:
        query = sql.SQL(f"CREATE DATABASE {database_name}")
        cursor.execute(query)
        
# Create Schema
def create_schema(connection, schema_name):
    connection.autocommit = True
    with connection.cursor() as cursor:
        query = sql.SQL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cursor.execute(query)

# Drop Database
def drop_database(connection, database_name):
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"DROP DATABASE {database_name}")

# Check if table exists
def table_exists(con, schema, table_str):
    exists = False
    try:
        with con.cursor() as cursor:
            cursor.execute("select exists(select * from information_schema.tables where table_schema = %s AND table_name=%s)", (schema, table_str,))
            exists = cursor.fetchone()[0]
    except psycopg.Error as e:
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
    except psycopg.Error as e:
        print(e)

    return col_names


# Insert Values into a table
def insert_into(connection, schema, table, data: pd.DataFrame):
    """_summary_

    Args:
        connection (_type_): _description_
        schema (_type_): _description_
        table (_type_): _description_
        data (_type_): _description_
    """
    try:
        with connection.cursor() as cursor:
            # Define query for importing into table of the same schema
            query = sql.SQL("INSERT INTO {schema}.{table} VALUES ({values})").format(
                schema = sql.Identifier(schema),
                table = sql.Identifier(table),
                values = sql.SQL(', ').join(sql.Placeholder() * len(data.columns))
            )
    
            # Turn into a tuple to insert into table
            data_list = [tuple(row) for row in data.itertuples(index=False)] 

            # Execute many to insert all records at once
            cursor.executemany(query, data_list)
    
    except psycopg.Error as e:
        print(e)