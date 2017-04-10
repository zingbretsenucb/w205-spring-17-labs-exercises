from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt

import psycopg2
from collections import defaultdict
import re

import ConfigParser
import os

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()

        file_path = os.path.dirname(os.path.realpath(__file__))
        parser = ConfigParser.ConfigParser()

        parser.read(os.path.join(file_path, 'credentials.config'))

        database = parser.get('TCOUNT', 'database')
        table = parser.get('TCOUNT', 'table')
        user = parser.get('POSTGRES', 'user')
        password = parser.get('POSTGRES', 'password')

        self.updater = pgUpdater(database = database, table = table,
                                 user = user, password = password,
                                 word_buffer_size = 50, count_buffer_size = 10)

    def process(self, tup):
        word = tup.values[0]
        
        # Use custom pgUpdater class to add words to the postgres
        # database in batches (or after each word)
        self.updater.add((word, 1))
        

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))




class pgUpdater(object):
    """Update database in batches 

    This class *should* be used as a context so that its database
    connection will be released gracefully. If not in a context, the
    class will connect and disconnect on every update or batch of
    updates. I don't see a way to use this as a context within Apache
    Storm, but for my own edification, I wanted to write this as a
    context.
    
    Rather than update the database upon receipt of every word, we
    maintain a buffer of words and their counts. When a single word
    has been encountered `count_buffer_size` numbers of time, we
    update the database entry for that word and delete the word from
    our buffer. 

    If, instead, we have `word_buffer_size` number of words in our
    buffer, we dump our full buffer to the database. This will make
    sure that our buffer sends the less common words to the database
    if it is taking them a long time to reach the `count_buffer_size`.

    The buffer sizes can be adjusted. By default, they are both set to
    1, so the database is, in fact, updated upon receipt of each
    word. The optimal values for each depend on the distribution of
    words and the freshness of data desired in the database.

    When the context is left, the class will dump all words in its
    buffer to the database. The user can manually dump the buffer, as
    well."""

    def __init__(self, database, table, word_buffer_size = 1,
                 count_buffer_size = 1, user = 'postgres',
                 password = 'postgres', host = 'localhost',
                 port = '5432'):

        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.word_buffer_size = word_buffer_size
        self.count_buffer_size = count_buffer_size

        self.in_context = False
        self.conn = None
        self.cur = None

        self.words = defaultdict(lambda: 0)


    def __enter__(self):
        """Open psql connection when entering context"""
        self.in_context = True
        self.connect()
        return self


    def __exit__(self, *args):
        """Safely close psql connection when exiting context"""
        try:
            self.dump_buffer()
        finally:
            self.conn.close()
        return False


    def reset(self, word):
        del self.words[word]


    def dump_buffer(self):
        for word in self.words.keys():
            try:
                self.update_db(word)
            finally:
                self.reset(word)


    def connect(self):
        if self.conn is None or self.conn.closed == 1:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port)

        self.cur = self.conn.cursor()


    def disconnect(self):
        self.conn.close()

    
    def add(self, word_tup):
        """Add word to buffer, and dump word/buffer if necessary"""
        word = word_tup[0]
        count = word_tup[1]

        self.words[word] += count

        if not self.in_context:
            self.connect()

        if self.words[word] >= self.count_buffer_size:
            self.update_db(word)
            self.reset(word)
        elif len(self.words) >= self.word_buffer_size:
            self.dump_buffer()

        if not self.in_context:
            self.disconnect()


    def update_db(self, word):
        count = self.words[word]
        word = re.sub("'", "''", word)

        insert_string = u'INSERT INTO tweetwordcount (word, count) \
            VALUES (\'{}\', {})'.format(word, count)
        update_string = u'UPDATE tweetwordcount set \
            count = count + {} \
            where word = \'{}\''.format(count, word)

        # Try to insert the word and its count
        try:
            self.cur.execute(insert_string)
        # If that fails because the word is already in the table,
        # update the value instead
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            self.cur.execute(update_string)
        # Either way, commit the changes
        finally:
            self.conn.commit()
