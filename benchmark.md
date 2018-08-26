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

![Micro-benchmarks](https://github.com/utsaslab/pebblesdb/blob/master/src/graphs/micro-all.png)
<p align="center"> Figure 1 - Micro-benchmarks </p>

#### Multi-threaded micro-benchmarks
* Figure 1 (c) shows the results of multi-threaded micro-benchmarks. We use 4 threads to insert 10M key-value pairs per thread with 
value size 1 KB and read 10M keys per thread. For the mixed workload, two threads write to the store and two threads read from 
the store. 
* We see trends similar to single-threaded benchmarks. PebblesDB has much higher throughput during pure write 
workload and comparable throuhgput in a pure read workload. For mixed workload, since PebblesDB finishes the writes faster, 
the pressure on Level 0 comes down earlier for PebblesDB and hence the overall throughput for read+write is higher for PebblesDB.

#### Other scenarios
* Figures 1 (d) to 1 (g) shows the benchmark results on different scenarios like a small cached datatset, small key-value pairs,  aged file-system and limited memory.
* For small cached dataset, PebblesDB-1 is the key-value stored configured with maximum of 1 file per guard. We see that setting
this to 1 helps in achieving comparable performance with other key-value stores.
* For the other scenarios like limited memory, aged file-system etc, the same pattern follows: PebblesDB typically achieves higher 
write throughput and comparable read throughput but has lesser range query performance. 

___

### YCSB Benchmark
[YCSB](https://github.com/brianfrankcooper/YCSB) is an industry-standard, widely used open source benchmark for NoSQL stores. We evaluate the different key-values stores using the YCSB benchmark suite. We inserted 50M key-value pairs (using 4 threads) with value size 1 KB totaling to around 52 GB of user data. Around 10M reads were performed for the other workloads.  

![YCSB-benchmark](https://github.com/utsaslab/pebblesdb/blob/master/src/graphs/multi-ycsb.png)

*Figure 2 - YCSB Benchmark*

* Figure 2 shows the performance results of YCSB benchmark on different key-value stores. We see that on write workloads like Load-A and Load-E, PebblesDB clearly has much higher throughput compared to the other key-value stores. Same is the case for Run-A which is a mixed read-write workload. 
* For other read dominated workloads, PebblesDB has comaparable throughput to HyperLevelDB. For Run-C, PebblesDB artificially has higher throughput and the reason turned out to be lesser number of files in PebblesDB and hence lesser number of table_cache misses. Increasing the table cache and re-running Run-C showed that the throughputs became comparable. 
* Surprisingly for Run-E, which is the range query workload, PebblesDB incurs only a small overhead. This is due to continuous (5%) writes happening in the workload which doesn't allow any key-value store to reach a fully compacted state as well as due to different number of next() operations (uniformly distributed between 1 to 100) after a seek, which amortizes the total disk amount of disk read.
* Also note that PebblesDB has the least cumulative write IO over the entire benchmark set.

___

### Real Applications
We evaluate PebblesDB using some real applications like HyperDex and MongoDB, by replacing their default storage engine with PebblesDB. The default storage engine for HyperDex is HyperLevelDB and the default storage engine for MongoDB is WiredTiger. MongoDB also supports RocksDB as the underlying storage engine. 

We insert 20M key-value pairs with value size 1 KB and do 10M operations (reads) over HyperDex/MongoDB. The client and the server were set up on the same host so that there is no network latency. We use 4 threads to perform the write/read operations over the applications. 

* Figure 3 shows the results of YCSB Benchmark on HyperDex. We see that PebblesDB still performs better in comparison to HyperLevelDB for write workload and has comparable performance for read throughput. But the amount of gain in write throughput has decreased compared to the benchmark on the key-value store itself. The reason is that HyperDex itself adds some latency to each write operation and also before each write, it does a read to check if the key is already present. This amortizes the overall time taken to write an entry and hence PebblesDB is not utilized to its maximum extent. 

![HyperDex-YCSB](https://github.com/utsaslab/pebblesdb/blob/master/src/graphs/hyperdex-multi.png)

*Figure 3 - YCSB Benchmark on HyperDex*

* Figure 4 shows the results of YCSB benchmark on MongoDB. We evaluate MongoDB on PebblesDB, RocksDB and WiredTiger. We see that PebblesDB has better write throughput and comparable read throughput overall and at the same time, doing lesser write IO compared to RocksDB. Similar to HyperDex, the gain in write throughput has come down due to similar reasons (application latency and read-write behavior). We believe the applications can be modified in order to make the best possible use of PebblesDB exploiting the high write throughput it provides. 

![MongoDB-YCSB](https://github.com/utsaslab/pebblesdb/blob/master/src/graphs/mongo.png)

*Figure 4 - YCSB Benchmark on MongoDB*

___

For any further details regarding the experiments/benchmarks or any other detailed analysis of the results, please take a look at the original paper [here](http://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf).
