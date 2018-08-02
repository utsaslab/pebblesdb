#!/bin/bash

cat $1 | grep 'Total time' | cut -d ' ' -f6 > bmlog_total_write_time

#cat $1 | grep 'Iteration' | cut -d ' ' -f12 > bmlog_sum_write_times

cat $1 | grep 'Time taken by SequenceWriteBegin' | cut -d ' ' -f5 > bmlog_seq_write_begin
cat $1 | grep 'Time taken by SequenceWriteEnd' | cut -d ' ' -f5 > bmlog_seq_write_end
cat $1 | grep 'Time taken by log.AddRecord' | cut -d ' ' -f5 > bmlog_log_addrecord
cat $1 | grep 'Time taken by InsertIntoVersion' | cut -d ' ' -f5 > bmlog_insert_into_version
cat $1 | grep 'Time taken to setsequence' | cut -d ' ' -f9 > bmlog_setsequence
cat $1 | grep 'Time taken by SetGuards' | cut -d ' ' -f5 > bmlog_setguards

#cat $1 | grep 'erase pending outputs' | awk -F 'outputs: ' '{print $2}' | cut -d ' ' -f1 > erase_pending_outputs
#cat $1 | grep 'Time taken to delete obsolete files' | awk -F 'files: ' '{print $2}' | cut -d ' ' -f1 > delete_obsolete_files


echo -n 'total_time_write: '
awk '{s+=$1} END {print s}' bmlog_total_write_time
echo 

#echo -n 'sum_of_individual_write_times: '
#awk '{s+=$1} END {print s}' bmlog_sum_write_times
#echo 

echo -n 'Time taken by SequenceWriteBegin: '
awk '{s+=$1} END {print s}' bmlog_seq_write_begin
echo -n 'Time taken by SequenceWriteEnd: '
awk '{s+=$1} END {print s}' bmlog_seq_write_end
echo -n 'Time taken by SetSequence and create new batch: '
awk '{s+=$1} END {print s}' bmlog_setsequence
echo -n 'Time taken by Set Guards: '
awk '{s+=$1} END {print s}' bmlog_setguards
echo -n 'Time taken by log.AddRecord: '
awk '{s+=$1} END {print s}' bmlog_log_addrecord
echo -n 'Time taken by InsertIntoVersion: '
awk '{s+=$1} END {print s}' bmlog_insert_into_version
