#!/usr/bin/env python
# -*- coding: utf-8 -*-

from FetchResults import *
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(
        description="initial settings for fetching results")

    parser.add_argument('word', metavar = 'W', type=str, nargs = '?',
                        help = "Word to search for (optional)")

    parser.add_argument('--desc', action = 'store_false', dest = 'asc',
                        help = "Sort descending (default: ascending)")

    parser.add_argument('--numerical', dest = 'num',
                        action = 'store_true',
                        help = "Sort by count (default: alphabetically)")

    parser.add_argument('--limit', type=int, nargs = '?', default = None,
                        help = "Limit number of results")


    args = parser.parse_args()
    print(args)

    asc  = args.asc
    word = args.word
    num  = 'count' if args.num else 'word'
    limit = args.limit

    with ResultsFetcher() as fetcher:
        if word is not None:
            fetcher.fetch_word(word)
        else:
            fetcher.fetch_all_words(sort_by = num, asc = asc, limit = limit)



if __name__ == '__main__':
    main()
