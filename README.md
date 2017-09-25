# Code coming soon! 

# Pebblesdb
PebblesDB is a write-optimized key-value store which is built using the novel FLSM (Fragmented Log-Structured Merge Tree) data structure. FLSM is a modification of standard LSM data structure which aims at achieving higher write throughput and lower write amplification without compromising on read throughput. 

PebblesDB is built by modifying HyperLevelDB (https://github.com/rescrv/HyperLevelDB) which, in turn, is built on top of LevelDB (https://github.com/google/leveldb). PebblesDB is API compatible with HyperLevelDB and LevelDB. Thus, PebblesDB is a *drop-in* replacement for LevelDB and HyperLevelDB.

If you are using LevelDB in your deployment, do consider trying out PebblesDB! PebblesDB can also be used to replace RocksDB as long as the RocksDB-specific functionality like column families are not used. 
