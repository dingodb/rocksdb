// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>
#include <sys/time.h>
#include <unistd.h>

#include "rocksdb/perf_context.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

std::string kDBPath = "/tmp/rocksdb_simple_example";

int main() {
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    options.compression = rocksdb::kNoCompression;

    long capacity = 1 * 1024 * 1024 * 1024;
    auto block_cache = rocksdb::NewLRUCache(capacity);
    auto compress_block_cache = rocksdb::NewClockCache(capacity);

    rocksdb::BlockBasedTableOptions tableOpts;
    tableOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    tableOpts.block_cache = block_cache;
    tableOpts.block_cache_compressed = compress_block_cache;  

    // tableOpts.whole_key_filtering = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));
    options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(3));


    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    int totalCnt = 10000;
    for (int i = 0; i < totalCnt; i++) {
        char buff[100];
        snprintf(buff, 100, "key%d", i);
        s = db->Put(WriteOptions(), buff, buff);
        assert(s.ok());
    }


    SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_perf_context()->Reset();

    ReadOptions localReadOpts;
    localReadOpts.total_order_seek = false;
    rocksdb::Slice upper_bound_slice = rocksdb::Slice("key999");
    localReadOpts.iterate_upper_bound = &upper_bound_slice;

    rocksdb::Iterator* iter = db->NewIterator(localReadOpts);

    for (iter->Seek("key991"); iter->Valid(); iter->Next()) {
        std::cout << iter->key().ToString() << ": " << iter->value().ToString() << std::endl;
    }

    std::cout << rocksdb::get_perf_context()->ToString() << std::endl;

    std::cout << "prefix checked: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_CHECKED) << std::endl;
    std::cout << "prefix useful: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_USEFUL) << std::endl;

  delete iter;
  delete db;
  return 0;
}


