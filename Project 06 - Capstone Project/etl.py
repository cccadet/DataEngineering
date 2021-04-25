import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, fact_insert


def load_staging_tables(cur, conn):
    """
    Load data into the staging tables.
    """
    print("Run copy_table_queries")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("copy_table_queries OK")    


def insert_tables(cur, conn):
    """
    Insert data from the staging tables to the dimensions tables.
    """
    print("Run insert_table_queries")
    for query in insert_table_queries:
        #print(query)
        cur.execute(query)
        conn.commit()
    print("insert_table_queries OK")
    
    
def insert_fact_tables(cur, conn):
    """
    Insert data from the staging tables to the fact tables.
    """
    print("Run fact_insert")
    for query in fact_insert:
        cur.execute(query)
        conn.commit()
    print("fact_insert OK")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    insert_fact_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()