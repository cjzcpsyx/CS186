#!/bin/bash


## compile the code
ant 
if [ $? -ne 0 ] 
then 
    exit 1
fi
java -jar dist/simpledb.jar convert data/data.txt 2 "int,int"
if [ $? -ne 0 ] 
then 
    exit 1
fi
java -jar dist/simpledb.jar parser data/catalog.txt
