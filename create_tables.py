import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops any pre-existing tables to be able to create them from scratch should they already exist.
    """
    print('Dropping tables')    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()



def create_tables(cur, conn):
    """
    Create staging and dimensional tables declared on sql_queries script.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Logs into the DB, runs both functions for drop and create tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()