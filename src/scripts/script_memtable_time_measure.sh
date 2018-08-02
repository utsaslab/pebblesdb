#!/bin/bash

cat $1 | grep 'Total time taken for memtable compaction' | awk -F 'compaction: ' '{print $2}' | cut -d ' ' -f1 > bmlog_total_time_memtable_compaction
cat $1 | grep 'Time taken for WriteLevel0Table' | awk -F 'Table: ' '{print $2}' | cut -d ' ' -f1 > bmlog_write_level0_table
cat $1 | grep 'add guards to edit' | awk -F 'edit: ' '{print $2}' | cut -d ' ' -f1 > bmlog_add_guards
cat $1 | grep 'add complete guards to edit' | awk -F 'edit: ' '{print $2}' | cut -d ' ' -f1 > bmlog_add_complete_guards
cat $1 | grep 'taken to LogAndApply' | awk -F 'LogAndApply: ' '{print $2}' | cut -d ' ' -f1 > bmlog_log_and_apply
cat $1 | grep 'erase pending outputs' | awk -F 'outputs: ' '{print $2}' | cut -d ' ' -f1 > bmlog_erase_pending_outputs
cat $1 | grep 'Time taken to delete obsolete files' | awk -F 'files: ' '{print $2}' | cut -d ' ' -f1 > bmlog_delete_obsolete_files

#write level 0 tables
cat $1 | grep 'BuildLevel0Table' | cut -d ' ' -f5 > bmlog_build_level0_table
cat $1 | grep 'get lock after building' | cut -d ' ' -f9 > bmlog_get_lock_after_building
cat $1 | grep 'add level0 files' | cut -d ' ' -f9 > bmlog_add_level0_files_to_edit

#LogAndApply
cat $1 | grep 'Time taken to save builder' | grep 'mtc' | cut -d ' ' -f11 > bmlog_save_builder
cat $1 | grep 'Time taken to finalize new version' | grep 'mtc' | cut -d ' ' -f9 > bmlog_finalize_version
cat $1 | grep 'Time taken to encode edit' | grep 'mtc' | cut -d ' ' -f8 > bmlog_encode_edit
cat $1 | grep 'Time taken to add record to descriptor log' | grep 'mtc' | cut -d ' ' -f11 > bmlog_add_record_desc_log
cat $1 | grep 'Time taken to sync manifest log write' | grep 'mtc' | cut -d ' ' -f10 > bmlog_sync_manifest_log
cat $1 | grep 'Time taken to append new version' | grep 'mtc' | cut -d ' ' -f9 > bmlog_append_new_version
cat $1 | grep 'Time taken to apply edit to builder'| grep 'mtc' | cut -d ' ' -f10 > bmlog_apply_edit

echo Overall
echo -n 'total_time_memtable_compaction: '
awk '{s+=$1} END {print s}' bmlog_total_time_memtable_compaction
echo 

echo First level split
echo -n 'write_level0_table: '
awk '{s+=$1} END {print s}' bmlog_write_level0_table
echo -n 'add_guards: '
awk '{s+=$1} END {print s}' bmlog_add_guards
echo -n 'add_complete_guards: '
awk '{s+=$1} END {print s}' bmlog_add_complete_guards
echo -n 'log_and_apply: '
awk '{s+=$1} END {print s}' bmlog_log_and_apply
echo -n 'erase_pending_outputs: '
awk '{s+=$1} END {print s}' bmlog_erase_pending_outputs
echo -n 'delete_obsolete_files: '
awk '{s+=$1} END {print s}' bmlog_delete_obsolete_files

echo 
echo WriteLevel0Tables
echo -n 'build level0 table: '
awk '{s+=$1} END {print s}' bmlog_build_level0_table
echo -n 'Get lock after building: '
awk '{s+=$1} END {print s}' bmlog_get_lock_after_building
echo -n 'add level0 files to edit: '
awk '{s+=$1} END {print s}' bmlog_add_level0_files_to_edit

echo
echo LogAndApply
echo -n 'apply edit to version: '
awk '{s+=$1} END {print s}' bmlog_apply_edit
echo -n 'save builder: '
awk '{s+=$1} END {print s}' bmlog_save_builder
echo -n 'finalize version: '
awk '{s+=$1} END {print s}' bmlog_finalize_version
echo -n 'encode edit: '
awk '{s+=$1} END {print s}' bmlog_encode_edit
echo -n 'add record to desc log: '
awk '{s+=$1} END {print s}' bmlog_add_record_desc_log
echo -n 'sync manifest log: '
awk '{s+=$1} END {print s}' bmlog_sync_manifest_log
echo -n 'append new version: '
awk '{s+=$1} END {print s}' bmlog_append_new_version

echo
echo -n 'Number of memtable compactions: '
cat $1 | grep 'Total time taken for memtable compaction' | wc -l
