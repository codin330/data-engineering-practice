import psycopg2
import csv
import datetime
import os

def get_sql_connection():
    ''' Retunr an sql connection
    '''

    #TODO migrate credentials to a configuration file
    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    return psycopg2.connect(host=host, database=database, user=user, password=pas)

def exec_sql_script(db_connection, sql_script_file:str) -> None:
    ''' Load and execute SQL script

    '''
    db_cursor = db_connection.cursor()
    with open(sql_script_file, 'r') as f:
        text = str(f.read())
        db_cursor.execute(text)
    db_connection.commit()

def load_accounts(db_connection, file:str) -> None:
    ''' Load accounts data from CSV file into accounts database table

    '''
    db_cursor = db_connection.cursor()
    sql_command = """INSERT INTO accounts 
        (customer_id, first_name, last_name, address_1, address_2, 
        city, state, zip_code, join_date) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        first_row = True
        for row in csv_reader:
            if first_row:
                first_row = False
            else:
                # convert csv values to expected database types
                fields = list(map(str.strip, row))
                fields[0] = int(fields[0])
                fields[8] = datetime.date.fromisoformat(fields[8].replace('/','-'))

                # insert into table
                db_cursor.execute(sql_command, fields)
        f.close()

    db_connection.commit()

def load_products(db_connection, file:str) -> None:
    ''' Load products data from CSV file into product database table

    '''
    db_cursor = db_connection.cursor()
    sql_command = """INSERT INTO products 
        (product_id, product_code, product_description) 
        VALUES (%s, %s, %s)"""
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        first_row = True
        for row in csv_reader:
            if first_row:
                first_row = False
            else:
                # convert csv values to expected database types
                fields = list(map(str.strip, row))
                fields[0] = int(fields[0])
                fields[1] = int(fields[1])

                # insert into table
                db_cursor.execute(sql_command, fields)
        f.close()

    db_connection.commit()

def load_transactions(db_connection, file:str) -> None:
    ''' Load transactions data from CSV file into transactions database table

    '''
    db_cursor = db_connection.cursor()
    sql_command = """INSERT INTO transactions 
        (transaction_id, transaction_date, product_id, product_code, product_description, 
        quantity, account_id) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        first_row = True
        for row in csv_reader:
            if first_row:
                first_row = False
            else:
                # convert csv values to expected database types
                fields = list(map(str.strip, row))
                fields[1] = datetime.date.fromisoformat(fields[1].replace('/','-'))
                fields[2] = int(fields[2])
                fields[3] = int(fields[3])
                fields[5] = int(fields[5])
                fields[6] = int(fields[6])

                # insert into table
                db_cursor.execute(sql_command, fields)
        f.close()

    db_connection.commit()    

def main():

    create_sql_script = './sql/create.sql'
    accounts_file = './data/accounts.csv'
    products_file = './data/products.csv'
    transactions_file = './data/transactions.csv'

    if 'data' not in os.listdir():
        os.chdir('./Exercises/Exercise-5')

    conn = get_sql_connection()

    # Create tables
    exec_sql_script(conn, create_sql_script)

    # Load data into tables
    load_accounts(conn, accounts_file)
    load_products(conn, products_file)
    load_transactions(conn, transactions_file)

    conn.close()

if __name__ == '__main__':
    main()
