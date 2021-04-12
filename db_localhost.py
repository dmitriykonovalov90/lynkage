import psycopg2


def start_db_localhost():
    conn = psycopg2.connect(
        database="api",
        user="dashboard",
        password="password",
        host='localhost',
        port=5433,
    )
    return conn


def stop_db_localhost(conn):
    conn.close()


