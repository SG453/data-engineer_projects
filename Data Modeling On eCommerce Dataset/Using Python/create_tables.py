import configparser
import psycopg2
from sql_queries import create_table_queries

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #drop_tables(cur, conn)
    create_tables(cur, conn)
    print('Staging tables created')
    conn.close()


if __name__ == "__main__":
    main()
	
