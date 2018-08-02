#!/bin/bash

cat $1 | grep 'get from version' | awk -F 'version: ' '{print $2}' | cut -d ' ' -f1 > get_from_version
cat $1 | grep 'ReadRandom' | awk -F 'key: ' '{print $2}' | cut -d ' ' -f1 > read_random_get
cat $1 | grep 'to get mutex' | awk -F 'mutex: ' '{print $2}' | cut -d ' ' -f1 > get_mutex
cat $1 | grep 'to ref mems' | awk -F 'mems: ' '{print $2}' | cut -d ' ' -f1 > ref_mems
cat $1 | grep 'memtable and imm' | awk -F 'imm: ' '{print $2}' | cut -d ' ' -f1 > mem_imm
cat $1 | grep 'lock mutex' | awk -F 'mutex: ' '{print $2}' | cut -d ' ' -f1 > lock_mutex
cat $1 | grep 'finishing unref' | awk -F 'unref: ' '{print $2}' | cut -d ' ' -f1 > unref
cat $1 | grep 'table_cache' | awk -F 'call: ' '{print $2}' | cut -d ' ' -f1 > get_from_table_cache
cat $1 | grep 'list of files to search' | awk -F 'search: ' '{print $2}' | cut -d ' ' -f1 > list_of_files
cat $1 | grep 'clear tmp2 and set g' | awk -F 'set g: ' '{print $2}' | cut -d ' ' -f1 > clear_tmp_set_g
cat $1 | grep 'Get to finish of the search' | awk -F 'search: ' '{print $2}' | cut -d ' ' -f1 > get_from_version_start_to_return
cat $1 | grep 'find the guard' | awk -F 'guard: ' '{print $2}' | cut -d ' ' -f1 > find_guard
cat $1 | grep 'check sentinel files' | cut -d ' ' -f9 > check_sentinel_files
cat $1 | grep 'check guard files' | cut -d ' ' -f9 > check_guard_files
cat $1 | grep 'sort sentinel files' | cut -d ' ' -f9 > sort_sentinel_files
cat $1 | grep 'sort guard files' | cut -d ' ' -f9 > sort_guard_files
cat $1 | grep 'set file_meta_data_map' | cut -d ' ' -f8 > set_file_meta_data_map



echo -n 'read_random_get: '
awk '{s+=$1} END {print s}' read_random_get
echo 

echo -n 'get_from_version: '
awk '{s+=$1} END {print s}' get_from_version
echo -n 'get_mutex: '
awk '{s+=$1} END {print s}' get_mutex
echo -n 'ref_mems: '
awk '{s+=$1} END {print s}' ref_mems
echo -n 'mem_imm: '
awk '{s+=$1} END {print s}' mem_imm
echo -n 'lock_mutex: '
awk '{s+=$1} END {print s}' lock_mutex
echo -n 'unref: '
awk '{s+=$1} END {print s}' unref
echo 

echo -n 'clear_tmp_set_g: '
awk '{s+=$1} END {print s}' clear_tmp_set_g
echo -n 'find_guard: '
awk '{s+=$1} END {print s}' find_guard
echo -n 'find list_of_files: '
awk '{s+=$1} END {print s}' list_of_files
echo -n 'table_cache_get: '
awk '{s+=$1} END {print s}' get_from_table_cache
echo

echo -n 'check_sentinel_files: '
awk '{s+=$1} END {print s}' check_sentinel_files
echo -n 'sort_sentinel_files: '
awk '{s+=$1} END {print s}' sort_sentinel_files
echo -n 'check_guard_files: '
awk '{s+=$1} END {print s}' check_guard_files
echo -n 'sort_guard_files: '
awk '{s+=$1} END {print s}' sort_guard_files
echo -n 'set file_meta_data_map: '
awk '{s+=$1} END {print s}' set_file_meta_data_map
echo

echo -n 'get_from_version_start_to_return: '
awk '{s+=$1} END {print s}' get_from_version_start_to_return


