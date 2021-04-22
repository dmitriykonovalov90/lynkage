import psycopg2


def start_db_naumen():
    connection = psycopg2.connect(
        database="etl",
        user="dceo",
        password="dceo",
        host='192.168.77.111',
        port=52318,
    )
    return connection


def stop_db_naumen(connection):
    connection.close()




