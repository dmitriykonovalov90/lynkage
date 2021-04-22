import psycopg2
from sshtunnel import SSHTunnelForwarder


def startdb():
    server = SSHTunnelForwarder(
        ('192.168.77.102', 22),
        ssh_username="dmitriy.konovalov",
        ssh_password="123456aA",
        remote_bind_address=('localhost', 5432),
        local_bind_address=('localhost', 6543),
        )

    server.start()
    conn = psycopg2.connect(
        database="navigator_uat_api_main",
        user="dashboard_user",
        password="password",
        host=server.local_bind_host,
        port=server.local_bind_port,
    )
    conn_node = psycopg2.connect(
        database="navigator_uat_node_main",
        user="dashboard_user",
        password="password",
        host=server.local_bind_host,
        port=server.local_bind_port,
    )
    return conn, conn_node, server


def stopdb(conn, conn_node, server):
    conn.close()
    conn_node.close()
    server.stop()
