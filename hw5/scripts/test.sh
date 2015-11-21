#!/bin/bash

## compile the code

ant 
if [ $? -ne 0 ] 
then 
    exit 1
fi

#add first table
java -jar dist/simpledb.jar convert data/data.txt 2 "int,int"
if [ $? -ne 0 ] 
then 
    exit 1
fi

#add second table
java -jar dist/simpledb.jar convert data/s.txt 3 "int,int,string"
if [ $? -ne 0 ] 
then 
    exit 1
fi

#compile and start non-interactive mode
java -jar dist/simpledb.jar parser data/catalog.txt -f $1 | tee temp_test.txt
diff temp_test.txt results.txt > diffs.txt
if [ $? -ne 0 ]
then
pass=false
echo -e "****** ERROR output differed! See diffs.txt"
else
echo -e "****** PASS"
fi
