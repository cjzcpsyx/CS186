#!/bin/bash

rm -rf your_output 2> /dev/null
mkdir your_output 2>/dev/null

rm -rf diffs 2> /dev/null
mkdir diffs 2> /dev/null

javac YelpQueries.java
java -classpath ".:sqlite-jdbc-3.8.11.1.jar" YelpQueries

pass=true

function test_query() {
	query=$1
	test_name=$2

	# First test if the view exists
	if ! (sqlite3 yelp_dataset.db "$query" &> your_output/$test_name.txt ) ; then
		pass=false
		echo -e "ERROR $test_name! See your_output/$test_name.txt"
	else
	    diff your_output/$test_name.txt expected_output/$test_name.txt > diffs/$test_name.txt
	    if [ $? -ne 0 ]
	    then
		pass=false
		echo -e "ERROR $test_name output differed! See diffs/$test_name.txt"
	    else
		echo -e "PASS $test_name"
	    fi
	fi
}

test_query "SELECT * FROM q1;" q1  
test_query "SELECT * FROM q2 ORDER BY name;" q2
test_query "SELECT * FROM q3 ORDER BY name, stars;" q3
test_query "SELECT * FROM q4 ORDER BY name;" q4
test_query "SELECT * FROM q5 ORDER BY name;" q5
test_query "SELECT * FROM q6;" q6
test_query "SELECT * FROM q7 ORDER BY name;" q7
test_query "SELECT * FROM q8;" q8
test_query "SELECT * FROM q9 ORDER BY name;" q9
test_query "SELECT * FROM q10;" q10

if $pass; then
echo -e "SUCCESS: Your queries worked on this dataset!"
fi
