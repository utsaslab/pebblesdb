// Copyright (c) 2013-2014, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperLevelDB nor the names of its contributors may
//       be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// HyperLevelDB
#include <hyperleveldb/cache.h>
#include <hyperleveldb/db.h>
#include <hyperleveldb/filter_policy.h>

// STL
#include <tr1/memory>

// po6
#include <po6/threads/thread.h>

// e
#include <e/popt.h>
#include <e/time.h>

// ygor
#include <ygor.h>

static long _done = 0;
static long _number = 1000000;
static long _threads = 1;
static long _backup = 0;
static long _write_buf = 64ULL * 1024ULL * 1024ULL;
static long _block_size = 4096;
static const char* _output = "benchmark.dat";
static const char* _dir = ".";

static void
backup_thread(leveldb::DB*, ygor_data_logger* dl);

static void
worker_thread(leveldb::DB*, ygor_data_logger* dl,
              const armnod_config* k,
              const armnod_config* v);

int
main(int argc, const char* argv[])
{
    // parse the command line
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('n', "number")
            .description("perform N operations against the database (default: 1000000)")
            .metavar("N")
            .as_long(&_number);
    ap.arg().name('t', "threads")
            .description("run the test with T concurrent threads (default: 1)")
            .metavar("T")
            .as_long(&_threads);
    ap.arg().name('o', "output")
            .description("output file for benchmark results (default: benchmark.dat)")
            .as_string(&_output);
    ap.arg().name('d', "db-dir")
            .description("directory for leveldb storage (default: .)")
            .as_string(&_dir);
    ap.arg().name('w', "write-buffer")
            .description("write buffer size (default: 64MB)")
            .as_long(&_write_buf);
    ap.arg().name('B', "block-size")
            .description("block size (default: 4KB)")
            .as_long(&_block_size);
    ap.arg().name('b', "backup")
            .description("perform a live backup every N seconds (default: 0 (no backup))")
            .as_long(&_backup);
    const std::auto_ptr<armnod_argparser> key_parser(armnod_argparser::create("key-"));
    const std::auto_ptr<armnod_argparser> value_parser(armnod_argparser::create("value-"));
    ap.add("Key Generation:", key_parser->parser());
    ap.add("Value Generation:", value_parser->parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    // open the LevelDB
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = _write_buf;
    opts.block_size = _block_size;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    leveldb::DB* db;
    leveldb::Status st = leveldb::DB::Open(opts, _dir, &db);

    if (!st.ok())
    {
        std::cerr << "could not open LevelDB: " << st.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    // setup the experiment
    ygor_data_logger* dl = ygor_data_logger_create(_output, 1000000, 1000);

    if (!dl)
    {
        std::cerr << "could not open log: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;
    std::vector<thread_ptr> threads;

    if (_backup > 0)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(backup_thread, db, dl)));
        threads.push_back(t);
        t->start();
    }

    for (long i = 0; i < _threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(worker_thread, db, dl,
                                              key_parser->config(), value_parser->config())));
        threads.push_back(t);
        t->start();
    }

    // do the experiment

    // tear it down
    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    if (ygor_data_logger_flush_and_destroy(dl) < 0)
    {
        std::cerr << "could not close log: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    // dump stats of the DB
    std::string tmp;
    if (db->GetProperty("leveldb.stats", &tmp)) std::cout << tmp << std::endl;
    delete db;
    return EXIT_SUCCESS;
}

#define BILLION (1000ULL * 1000ULL * 1000ULL)

void
backup_thread(leveldb::DB* db, ygor_data_logger* dl)
{
    uint64_t target = e::time() / BILLION;
    target += _backup;
    uint64_t idx = 0;

    while (__sync_fetch_and_add(&_done, 0) < _number)
    {
        uint64_t now = e::time() / BILLION;

        if (now < target)
        {
            timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 250ULL * 1000ULL * 1000ULL;
            nanosleep(&ts, NULL);
        }
        else
        {
            target = now + _backup;
            char buf[32];
            snprintf(buf, 32, "%05lu", idx);
            buf[31] = '\0';
            leveldb::Slice name(buf);
            leveldb::Status st;
            ygor_data_record dr;
            dr.flags = 4;

            ygor_data_logger_start(dl, &dr);
            st = db->LiveBackup(name);
            ygor_data_logger_finish(dl, &dr);
            ygor_data_logger_record(dl, &dr);
            assert(st.ok());
            ++idx;
        }
    }
}

void
worker_thread(leveldb::DB* db,
              ygor_data_logger* dl,
              const armnod_config* _k,
              const armnod_config* _v)
{
    armnod_generator* key(armnod_generator_create(_k));
    armnod_generator* val(armnod_generator_create(_v));
        const char* v = armnod_generate(val);
        size_t v_sz = strlen(v);
    armnod_seed(key, pthread_self());
    armnod_seed(val, pthread_self());

    while (__sync_fetch_and_add(&_done, 1) < _number)
    {
        const char* k = armnod_generate(key);
        size_t k_sz = strlen(k);

        // issue a "get"
        //std::string tmp;
        //leveldb::ReadOptions ropts;
        ygor_data_record dr;
        //dr.flags = 1;
        //ygor_data_logger_start(dl, &dr);
        //leveldb::Status rst = db->Get(ropts, leveldb::Slice(k, k_sz), &tmp);
        //ygor_data_logger_finish(dl, &dr);
        //ygor_data_logger_record(dl, &dr);
        //assert(rst.ok() || rst.IsNotFound());

        // issue a "put"
        leveldb::WriteOptions wopts;
        wopts.sync = false;
        dr.flags = 2;
        ygor_data_logger_start(dl, &dr);
        leveldb::Status wst = db->Put(wopts, leveldb::Slice(k, k_sz), leveldb::Slice(v, v_sz));
        ygor_data_logger_finish(dl, &dr);
        ygor_data_logger_record(dl, &dr);
        assert(wst.ok());
    }

    armnod_generator_destroy(key);
    armnod_generator_destroy(val);
}
