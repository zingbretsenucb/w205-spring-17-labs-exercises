#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import ConfigParser
import os
import re
import sys

parser = ConfigParser.ConfigParser()

file_path = os.path.dirname(os.path.realpath(__file__))
parser.read(os.path.join(file_path, 'postgres_credentials.config'))

database = parser.get('TCOUNT', 'database')
table    = parser.get('TCOUNT', 'table')
user     = parser.get('POSTGRES', 'user')
password = parser.get('POSTGRES', 'password')
host     = parser.get('POSTGRES', 'host')
port     = parser.get('POSTGRES', 'port')

class ResultsFetcher(object):
    def __init__(self):
        self.conn = None
        self.cur = None


    def __enter__(self):
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port)
        self.cur = self.conn.cursor()
        return self


    def __exit__(self, *args):
        self.conn.close()
        return False


    def fetch_word(self, word):
        word = re.sub("'", "''", word)
        query_str = 'SELECT count from {} where word = \'{}\''.format(table, word)

        self.cur.execute(query_str)
        self.conn.commit()
        try:
            count = self.cur.fetchone()[0]
        except TypeError as e:
            count = 0

        output_str = 'Total number of occurrences of "{}": {}'
        print(output_str.format(word, count))


    def fetch_all_words(self):
        query_str = 'SELECT word, count from {}'.format(table)
        self.cur.execute(query_str)
        self.conn.commit()

        all_words = self.cur.fetchall()
        return all_words


    def print_all_words(self):
        all_words = self.fetch_all_words()
        self.print_words(all_words)


    def print_words(self, words):
        for word_count in words:
            print(word_count)


    def histogram(self, lb, ub):
        query_str = 'SELECT word, count from {} where count >= {} and count <= {}'.format(table, lb, ub)
        self.cur.execute(query_str)
        self.conn.commit()

        all_words = self.cur.fetchall()
        self.print_words(all_words)
