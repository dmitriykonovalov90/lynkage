import psycopg2


def start_db_naumen():
    connection = psycopg2.connect(
        database="rostel",
        user="dceo",
        password="dceo",
        host='192.168.77.151',
        port=5432,
    )
    return connection


def stop_db_naumen(connection):
    connection.close()




