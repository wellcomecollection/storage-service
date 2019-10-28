# -*- encoding: utf-8

import pyodbc


def get_connection():
    server = 'wt-dhaka'
    database = 'WELLCOME_SDB4'
    driver = '{ODBC Driver 17 for SQL Server}'

    return pyodbc.connect(
        driver=driver,
        server=server,
        database=database,
        trusted_connection='yes'
    )


def get_iterator(cnxn, *, query):
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Collection")

    for row in cursor.fetchall():
        yield {
            desc[0]: value
            for desc, value in zip(cursor.description, row)
        }
