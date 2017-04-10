#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import psycopg2
import ConfigParser
import os
import re
import sys

class ResultsFetcher(object):
    """Class to safely retrieve words from postgres"""

    def __init__(self, print_only = True):
        self.conn = None
        self.cur = None

        self.print_only = print_only

        # Load in details from credentials file
        parser = ConfigParser.ConfigParser()

        file_path = os.path.dirname(os.path.realpath(__file__))
        parser.read(os.path.join(file_path, 'postgres_credentials.config'))

        self.database = parser.get('TCOUNT', 'database')
        self.table    = parser.get('TCOUNT', 'table')
        self.user     = parser.get('POSTGRES', 'user')
        self.password = parser.get('POSTGRES', 'password')
        self.host     = parser.get('POSTGRES', 'host')
        self.port     = parser.get('POSTGRES', 'port')



    def __enter__(self):
        """Connect to database upon entering context"""

        self.connect()
        return self


    def __exit__(self, *args):
        """Safely close connection upon leaving context"""

        return self.close()


    def connect(self):
        """Connect to database"""

        self.conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        self.cur = self.conn.cursor()


    def close(self):
        """Close connection to database"""

        self.conn.close()   
        return False


    def fetch_word(self, word):
        """Fetch a single word from the database"""

        word = re.sub("'", "''", word)
        query_str = 'SELECT count from {} where word = \'{}\''.format(self.table, word)

        self.cur.execute(query_str)
        self.conn.commit()
        try:
            count = self.cur.fetchone()[0]
        except TypeError as e:
            count = 0

        output_str = 'Total number of occurrences of "{}": {}'

        if self.print_only:
            print(output_str.format(word, count))
        else:
            return (word, count)


    def fetch_all_words(self, sort_by = 'word', asc = True):
        """Fetch all words from the database"""

        query_str = 'SELECT word, count from {}'.format(self.table)

        valid_sorts = ('word', 'count')

        if sort_by in valid_sorts:
            order = ' ASC' if asc else ' DESC'
            query_str += ' order by {} {}'.format(sort_by, order)
            
        self.cur.execute(query_str)
        self.conn.commit()

        words = self.cur.fetchall()

        if self.print_only:
            print(*words, sep = '\n')
        else:
            return words


    def histogram(self, lb, ub):
        """Fetch all words from the database that appear a certain number of times"""

        query_str = 'SELECT word, count from {} where count >= {} and count <= {}'.format(
            self.table, lb, ub)

        self.cur.execute(query_str)
        self.conn.commit()

        words = self.cur.fetchall()

        if self.print_only:
            for word in words:
                print('{}: {}'.format(word[0], word[1]))
        else:
            return words
