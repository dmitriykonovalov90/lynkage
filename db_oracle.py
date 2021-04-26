import cx_Oracle


def start_db_oracle():
    connection_ora = cx_Oracle.connect("AGP_LN_OUTSIDE", "20LN18TY43", "192.168.77.111:53002/ACOMIS",
                                   encoding="UTF-8")
    # connection_ora = cx_Oracle.connect(
    #     database="AGENT_PAY",
    #     user="AGP_LN_OUTSIDE",
    #     password="20LN18TY43",
    #     host='192.168.77.111',
    #     port=53002,
    # )


    return connection_ora


def stop_db_oracle(connection_ora):
    connection_ora.close()
