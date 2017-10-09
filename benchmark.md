## Performance Benchmarks
We evaluate and benchmark on PebblesDB using different types of benchmarks like micro-benchmarks, macro-benchmarks, 
benchmarks with real applications etc and compare the performance with other similiar key-value strores like LevelDB, 
HyperLevelDB and RocskDB. The list of benchmarks and the performance results are as follows:

### Micro-benchmarks
LevelDB ships with a `db_bench` tool which is used to perform different micro-benchmarks. The following graphs shows the peformance
numbers for a list of different types of micro-benchmarks conducted. The same `db_bench` tool was used to evaluate all
the key-value stores evaluated. 

#### Write amplification
Figure 1 (a) shows the write amplification and the amount of write IO (GB) when 10M, 100M and 500M keys with value size 128 bytes 
are written to the different key-value stores. We note that FLSM does much lesser write IO in comparison with other key-value
stores.

#### Single-threaded micro-benchmarks
Figure 1 (b) shows a set of single threaded micro-benchmarks. For each of these experiments, 50M key-value pairs were inserted
with value size of 1 KB (totaling to around 34 GB due to updates to the same key) and a memtable of 4MB (default of LevelDB) 
was used. Each of the different benchmark is explained 
below:  

* **Seq-Writes:** Inserts sequential keys (say, from 1 to 50M) into the key-value stores. PebblesDB suffers a lower throughput in 
this benchmark because due to the presence of guards in the higher levels, the files have to be split during compaction in 
PebblesDB while the other key-value stores simply move the files to the higher levels without rewriting them. 

* **Random-Writes:** Inserts random keys into the key-value stores. PebblesDB has a higher random-write throughput because it does
lesser write IO and hence the background compaction in PebblesDB is much faster than other key-value stores, indirectly helping
in achieving high write throughput. 

* **Reads:** Reads random keys from the key-value stores. PebblesDB has a comparable (slightly higher) throughput with other key-value
stores for reads because it uses sstable level bloom filters and has larger files. Due to sstable level bloom filter, multiple 
files within a guard doesn't affect the read latency and due to larger files, there is lesser number of table_cache (in memory cache
of open file handles) misses. 

* **Range-Queries:** PebblesDB has lesser seek throughput compared to other key-value stores because sstable level bloom filters
do not help in solving the problem of multiple files within a guard. Note that this is a worst case benchmark where there is only
Seek operation and no next operations following.  

* **Deletes:** In LSM stores, deletes are similar to writes except that we insert only the key with a deletion flag. Hence, 
PebblesDB achieves the same throughput gain as Random-Writes. 

![Micro-benchmarks](https://github.com/utsaslab/pebblesdb/blob/master/graphs/micro-all.png)
<p align="center"> Figure 1 - Micro-benchmarks </p>

#### Multi-threaded micro-benchmarks
* Figure 1 (c) shows the results of multi-threaded micro-benchmarks. We use 4 threads to insert 10M key-value pairs per thread with 
value size 1 KB and read 10M keys per thread. For the mixed workload, two threads write to the store and two threads read from 
the store. 
* We see trends similar to single-threaded benchmarks. PebblesDB has much higher throughput during pure write 
workload and comparable throuhgput in a pure read workload. For mixed workload, since PebblesDB finishes the writes faster, 
the pressure on Level 0 comes down earlier for PebblesDB and hence the overall throughput for read+write is higher for PebblesDB.

#### Other scenarios
* Figures 1 (d) to 1 (g) shows the benchmark results on different scenarios like a small cached datatset, limited memory etc. 
* For small cached dataset, PebblesDB-1 is the key-value stored configured with maximum of 1 file per guard. We see that setting
this to 1 helps in achieving comparable performance with other key-value stores.
* For the other scenarios like limited memory, aged file-system etc, the same pattern follows: PebblesDB typically achieves higher 
write throughput and comparable read throughput but has lesser range query performance. 



