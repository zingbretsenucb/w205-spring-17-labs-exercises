#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import ConfigParser

def main():
    parser = ConfigParser.ConfigParser()
    parser.read('extweetwordcount/credentials.config')

    try:
        database  = parser.get('POSTGRES' , 'database')
        user      = parser.get('POSTGRES' , 'user')
        password  = parser.get('POSTGRES' , 'password')
        host      = parser.get('POSTGRES' , 'host')
        port      = parser.get('POSTGRES' , 'port')
        tcount_db = parser.get('TCOUNT'   , 'database')
        table     = parser.get('TCOUNT'   , 'table')

    except ConfigParser.NoSectionError as e:
        print("##############################")
        print("Warning: Improperly formatted config file.")
        print("Please create a file in `extweetwordcount` called `credentials.config`")
        print("with your desired database configuration details")
        print("##############################")
        print("Please also make sure to enter your twitter credentials in that file")
        print("")
        raise e

    conn = pg.connect(database = database,
                            user     = user,
                            password = password,
                            host     = host,
                            port     = port)

    try:
        # CREATE DATABASE can't run inside a transaction
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE {}".format(tcount_db))
        cur.close()
        conn.close()
    except:
        print "Could not create {}".format(tcount_db)

    conn = pg.connect(database = tcount_db,
                      user     = user,
                      password = password,
                      host     = host,
                      port     = port)


    cur = conn.cursor()

    try:
        cur.execute('''CREATE TABLE {}
            (word TEXT PRIMARY KEY     NOT NULL,
            count INT     NOT NULL);'''.format(table))
        conn.commit()
    finally:
        conn.close()
    

if __name__ == '__main__':
    main()
