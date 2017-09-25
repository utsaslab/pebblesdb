# Code will be added to the repository soon


# pebblesdb
PebblesDB is a write-optimized key-value store which is built using FLSM (Fragmented Log-Structured Merge Tree) data structure. FLSM is a modification of standard LSM data structure which aims at achieving higher write throughput and lower write amplification without compromising on read throughput. 

PebblesDB is built by modifying HyperLevelDB (https://github.com/rescrv/HyperLevelDB) which, in turn, is built on top of LevelDB (https://github.com/google/leveldb). Hence, PebblesDB is API compatible with HyperLevelDB and LevelDB. 
