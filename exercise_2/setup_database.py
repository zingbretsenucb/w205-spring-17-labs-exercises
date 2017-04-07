#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import ConfigParser

def main():
    # So we can read the config file
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
        print("Please create a file in exercise_2/")
        print("called `postgres_credentials.config`")
        print("with your desired database configuration details")
        print("and then run `setup_config.sh` in exercise_2/")
        print("##############################")
        print("")
        raise e

    # Forge a strong connection with postgres
    conn = pg.connect(database = database,
                            user     = user,
                            password = password,
                            host     = host,
                            port     = port)

    try:
        # Nothing can keep us apart
        # CREATE DATABASE can't run inside a transaction
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE {}".format(tcount_db))
        cur.close()
        conn.close()
    except:
        # Well, something might. If it does...
        print "Could not create {}".format(tcount_db)

    # Connect to our new database
    conn = pg.connect(database = tcount_db,
                      user     = user,
                      password = password,
                      host     = host,
                      port     = port)


    cur = conn.cursor()

    # And create our table
    try:
        cur.execute('''CREATE TABLE {}
            (word TEXT PRIMARY KEY     NOT NULL,
            count INT     NOT NULL);'''.format(table))
        conn.commit()
    finally: # ~~FIN~~ #
        conn.close()
    

if __name__ == '__main__':
    main()
