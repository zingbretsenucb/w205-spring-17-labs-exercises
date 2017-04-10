#!/usr/bin/env python
# -*- coding: utf-8 -*-

from FetchResults import *
import sys

def main():
    with ResultsFetcher() as fetcher:
        try:
            word = sys.argv[1]
            fetcher.fetch_word(word)
        except:
            fetcher.fetch_all_words()


if __name__ == '__main__':
    main()
