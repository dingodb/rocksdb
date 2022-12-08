#include <cstdlib>

#include <iostream>
#include <fstream>
#include <string>

#include <sys/time.h>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "rocksdb/compaction_filter.h"


using namespace std;


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::NewBloomFilterPolicy;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using ROCKSDB_NAMESPACE::NewCappedPrefixTransform;
using ROCKSDB_NAMESPACE::get_perf_context;
using ROCKSDB_NAMESPACE::SetPerfLevel;
using ROCKSDB_NAMESPACE::CompactRangeOptions;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::CompactionFilter;



// 测试version相关代码
void testVersion(const string db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;
    // options.disable_auto_compactions = true;
    // options.write_buffer_size = 4 * 1000 * 1000;
    // options.max_write_buffer_number = 5;
    // options.level0_file_num_compaction_trigger = 4;
    // options.max_bytes_for_level_base = 64 * 1000 * 1000;
    // options.max_bytes_for_level_multiplier = 4;
    // options.target_file_size_base = 16 * 1000 * 1000;
    // options.num_levels = 4;

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    sleep(1000);
    // 关闭
    db->Close();
    delete db;
}

void testBlockCacheIndexAndFilter(const string db_path) {
    Options options;
    options.info_log_level = ROCKSDB_NAMESPACE::InfoLogLevel::INFO_LEVEL;
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 打开DB
    DB* db;
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    // db->Put(WriteOptions(), "key", "v1");
    // db->Flush(FlushOptions());

    // cout << "BLOCK_CACHE_ADD: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_ADD) << endl;
    // cout << "BLOCK_CACHE_INDEX_MISS: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_INDEX_MISS) << endl;
    // cout << "BLOCK_CACHE_INDEX_HIT: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_INDEX_HIT) << endl;
    // cout << "BLOCK_CACHE_FILTER_MISS: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_FILTER_MISS) << endl;
    // cout << "BLOCK_CACHE_FILTER_HIT: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_FILTER_HIT) << endl;

    // string value;
    // s = db->Get(ReadOptions(), "key", &value);

    cout << "BLOCK_CACHE_ADD: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_ADD) << endl;

    cout << "BLOCK_CACHE_INDEX_ADD: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_INDEX_ADD) << endl;
    cout << "BLOCK_CACHE_INDEX_MISS: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_INDEX_MISS) << endl;
    cout << "BLOCK_CACHE_INDEX_HIT: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_INDEX_HIT) << endl;

    cout << "BLOCK_CACHE_FILTER_ADD: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_FILTER_ADD) << endl;
    cout << "BLOCK_CACHE_FILTER_MISS: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_FILTER_MISS) << endl;
    cout << "BLOCK_CACHE_FILTER_HIT: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOCK_CACHE_FILTER_HIT) << endl;

    // 关闭
    db->Close();
    delete db;
}


int main(int argc, char *argv[]) {
    // testVersion("/tmp/compact_test_01");
    testBlockCacheIndexAndFilter("/tmp/compact_block_cache_index_filter");

    return 0;
}