#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

# This is a reducer for count characters and count words, they are the same!

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)

    # count the most and least appear char
    most_count = 0
    least_count = sys.maxint
    most_word = []
    least_word = []

    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print "%s%s%d" % (current_word, separator, total_count)

            if total_count == most_count:
                most_word.append(current_word)

            if total_count > most_count:
                most_count = total_count
                del most_word[:]
                most_word.append(current_word)

            if total_count == least_count:
                least_word.append(current_word)

            if total_count < least_count:
                least_count = total_count
                del least_word[:]
                least_word.append(current_word)

        except ValueError:
            # count was not a number, so silently discard this item
            pass

    print("Char with max frequency = %d is :" % most_count)
    print(most_word)

    print("Char with min frequency = %d is :" % least_count)
    print(least_word)

if __name__ == "__main__":
    main()