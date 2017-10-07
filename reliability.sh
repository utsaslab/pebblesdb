#!/bin/bash

for i in `seq 1 10`;
do
	echo ----------------------------- Iteration $i -------------------------------
	export file_name='rel_inserted_keys'
	echo Starting FillRandom . . 
	nohup ./db_bench --benchmarks=rel_start &
	pid=`pgrep bench`
	seconds=`shuf -i 1-20 -n 1`
	echo Waiting for $seconds seconds
	sleep $seconds

	echo Killing process $pid
	kill $pid
	echo Process killed. 
	sleep 5
	
	echo --------------------------------------------------------------------------
	echo Starting Reliablity Check . . 
	export file_name='rel_inserted_keys'
	./db_bench --benchmarks=rel_check --use_existing_db=1
	echo Reliability check complete. Please check logs for any issues.
	echo --------------------------------------------------------------------------
	
done

