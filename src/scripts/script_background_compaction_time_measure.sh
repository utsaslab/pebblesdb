#!/bin/bash

cat $1 | grep 'Time taken to complete BackgroundCompaction' | awk -F 'BackgroundCompaction: ' '{print $2}' | cut -d ' ' -f1 > total_time_background_compaction
cat $1 | grep 'Time taken to pick compaction level:' | awk -F 'level: ' '{print $2}' | cut -d ' ' -f1 > pick_compaction_level
cat $1 | grep 'Time taken to pick compaction:' | awk -F 'compaction: ' '{print $2}' | cut -d ' ' -f1 > pick_compaction
cat $1 | grep 'Time taken to add guards to edit' | awk -F 'edit: ' '{print $2}' | cut -d ' ' -f1 > add_guards
cat $1 | grep 'Time taken to complete DoCompactionWorkGuards' | awk -F 'WorkGuards: ' '{print $2}' | cut -d ' ' -f1 > do_compaction_work_guards
cat $1 | grep 'Time taken to cleanup compaction and delete obsolete files' | awk -F 'files: ' '{print $2}' | cut -d ' ' -f1 > cleanup_compaction

cat $1 | grep 'Time taken to iterate keys and split them into files' | awk -F 'files: ' '{print $2}' | cut -d ' ' -f1 > iterate_keys_and_split
cat $1 | grep 'collect stats' | awk -F 'stats: ' '{print $2}' | cut -d ' ' -f1 > collect_stats
cat $1 | grep 'Time taken to get the lock' | awk -F 'lock: ' '{print $2}' | cut -d ' ' -f1 > bg_compaction_get_the_lock
cat $1 | grep 'Time taken to install compaction results' | awk -F 'results: ' '{print $2}' | cut -d ' ' -f1 > install_compaction_results


echo -n 'total_time_background_compaction: '
awk '{s+=$1} END {print s}' total_time_background_compaction
echo

echo -n 'pick_compaction_level: '
awk '{s+=$1} END {print s}' pick_compaction_level
echo -n 'pick_compaction: '
awk '{s+=$1} END {print s}' pick_compaction
echo -n 'add_guards: '
awk '{s+=$1} END {print s}' add_guards
echo -n 'do_compaction_work_guards: '
awk '{s+=$1} END {print s}' do_compaction_work_guards
echo -n 'cleanup_compaction: '
awk '{s+=$1} END {print s}' cleanup_compaction
echo

echo -n 'iterate_keys_and_split: '
awk '{s+=$1} END {print s}' iterate_keys_and_split
echo -n 'collect_stats: '
awk '{s+=$1} END {print s}' collect_stats
echo -n 'bg_compaction_get_the_lock: '
awk '{s+=$1} END {print s}' bg_compaction_get_the_lock
echo -n 'install_compaction_results: '
awk '{s+=$1} END {print s}' install_compaction_results
