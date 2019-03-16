#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys
import os

sc = SparkContext()
path = ''
textFind = str(sys.argv[1])
text_File = sc.wholeTextFiles(path)
temp = text_File.map(lambda x: (x[0].split('/')[-2], x[1].split())).collect()
pairs = []

for item in temp:
    for word in item[1]:
        if (word == textFind)
            pairs.append((item[0], word))

input(pairs)

words = text_File.flatMapValues(lambda x: x.lower().split()).map(lambda x: (x[0].split('/')[-2], x[1])).filter(
    lambda x: x[1] == textFind)
wordCounts = pairs.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False).take(5)

print('Слово для поиска: ' + str(textFind))

for x in wordCounts:
    print(x)

input("Press ctrl+с to exit")
