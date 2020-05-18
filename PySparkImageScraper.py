# -*- coding: utf-8 -*-
import re
import requests
from pyspark.sql import SparkSession
if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("HW5DATA603") \
        .getOrCreate()
sc = scSpark.sparkContext
driver = requests.get('https://shadygrove.umd.edu/')
content = driver.content
sdfData = sc.parallelize([str(content)])
MapData = sdfData.flatMap(lambda x: re.findall('(http:|https:)([/|.|\w|\s|-]*)(.jpg|.gif|.png|.jpeg|.bmp|.svg)',x))
res = [''.join(tups) for tups in MapData.collect()]
print("There are {0} images at the {1} site".format(MapData.count(),driver.url))
print("The images are:") 
print(*res, sep = "\n") 
sc.stop()