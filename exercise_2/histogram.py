#!/usr/bin/env python
# -*- coding: utf-8 -*-

from FetchResults import *
import sys

def main():
    with ResultsFetcher() as fetcher:
        try:
            lb, ub = sys.argv[1].split(',')
            fetcher.histogram(lb, ub)
        except:
            print("You must supply a lowerbound and upperbound")
            print("e.g., python histogram.py 3,5")


if __name__ == '__main__':
    main()
