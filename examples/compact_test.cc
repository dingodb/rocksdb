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

const char alphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 
    'q', 'r', 's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// 生成随机字符串
string genRandomString(int len) {
    string result;
    int alphabet_len = sizeof(alphabet);
    for (int i = 0; i < len; ++i) {
        result.append(1, alphabet[rand() % alphabet_len]);
    }

    return result;
}

// 分割字符串
vector<string> splitStr(string str, const string &delim) {
    vector<string> vec;
    int nPos = str.find(delim.c_str());
    while (nPos != -1) {
        string temp = str.substr(0, nPos);
        vec.push_back(temp);
        str = str.substr(nPos + 1);
        nPos = str.find(delim.c_str());
    }
    vec.push_back(str);

    return vec;
}


// 写数据到DB
void writeData(DB* db, const string filepath) {
    ifstream ifs;
    ifs.open(filepath, ios::in);

    char buf[128] = {0};
    while (ifs.getline(buf, sizeof(buf))) {
        vector<string> vec = splitStr(string(buf), ",");
        if (vec.size() < 2) {
            continue;
        }

        Status s = db->Put(WriteOptions(), vec[0], vec[1]);
        assert(s.ok());
    }
}

// 得到当前时间-微秒
uint64_t getCurrentTime() {
    struct timeval val;
    gettimeofday(&val, nullptr);
    return val.tv_sec * 1000000 + val.tv_usec;
}

void countIter(DB* db) {
    uint64_t start_time = getCurrentTime();
    ReadOptions read_options;
    Iterator* it = db->NewIterator(read_options);
    int count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ++count;
        // cout << "key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
    }
    delete it;
    cout << "count: " << count << endl;
    cout << "scan used time: " << getCurrentTime() - start_time << " us" << endl;
}


// 测试DeleteFilesInRange
void testDeleteFilesInRange(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;


    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "delete_file_in_range") {
        uint64_t start_time = getCurrentTime();
        // 删除数据，然后compact
        Slice start = "";
        Slice end = "z";
        Status s = ROCKSDB_NAMESPACE::DeleteFilesInRange(db, db->DefaultColumnFamily(), &start, &end);
        assert(s.ok());

        cout << "DeleteFilesInRange used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
    }

    string result;
    db->GetProperty("rocksdb.stats", &result);
    cout << "stats: " << result << endl;

    // 关闭
    db->Close();
    delete db;
}


// 测试compaction
void testCompaction01(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "delete_and_compact") {
        uint64_t start_time = getCurrentTime();
        // 删除数据，然后compact
        Slice start = "0000000000000000";
        Slice end = "zzzzzzzzzzzzzzzz";
        Status s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());

        cout << "DeleteRange used time: " << getCurrentTime() - start_time << " us" << endl;
        start_time = getCurrentTime();

        CompactRangeOptions cr_option = CompactRangeOptions();
        cr_option.max_subcompactions = 1;
        s = db->CompactRange(cr_option, &start, &end);
        assert(s.ok());

        cout << "CompactRange used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
    }

    string result;
    db->GetProperty("rocksdb.stats", &result);
    cout << "stats: " << result << endl;

    // 关闭
    db->Close();
    delete db;
}


// 先DeleteRange全部数据，再多次compact
void testCompaction02(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 9;

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "delete_and_compact") {
        uint64_t start_time = getCurrentTime();
        // 删除数据，然后compact
        Slice start = "0000000000000000";
        Slice end = "zzzzzzzzzzzzzzzz";
        Status s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());

        cout << "DeleteRange used time: " << getCurrentTime() - start_time << " us" << endl;
        start_time = getCurrentTime();

        CompactRangeOptions cr_option = CompactRangeOptions();
        cr_option.max_subcompactions = 9;
        Slice compact_start = "0000000000000000";
        Slice compact_end = "eeeeeeeeeeeeeeee";
        s = db->CompactRange(cr_option, &compact_start, &compact_end);
        assert(s.ok());

        cout << "CompactRange 0-e used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        compact_start = "eeeeeeeeeeeeeeee";
        compact_end =  "llllllllllllllll";
        s = db->CompactRange(cr_option, &compact_start, &compact_end);
        assert(s.ok());
        cout << "CompactRange e-l used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        compact_start = "llllllllllllllll";
        compact_end =  "rrrrrrrrrrrrrrrr";
        s = db->CompactRange(cr_option, &compact_start, &compact_end);
        assert(s.ok());
        cout << "CompactRange l-r used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        compact_start = "rrrrrrrrrrrrrrrr";
        compact_end =  "zzzzzzzzzzzzzzzz";
        s = db->CompactRange(cr_option, &compact_start, &compact_end);
        assert(s.ok());
        cout << "CompactRange r-z used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
    }

    string result;
    db->GetProperty("rocksdb.stats", &result);
    cout << "stats: " << result << endl;

    // 关闭
    db->Close();
    delete db;
}

// 先DeleteRange部分数据，再compact，依次循环
void testCompaction03(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 9;


    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "delete_and_compact") {
        uint64_t start_time = getCurrentTime();
        // 删除数据，然后compact
        Slice start = "";
        Slice end = "";

        CompactRangeOptions cr_option = CompactRangeOptions();
        cr_option.max_subcompactions = 9;

        start = "0000000000000000";
        end = "eeeeeeeeeeeeeeee";
        Status s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());
        s = db->CompactRange(cr_option, &start, &end);
        assert(s.ok());
        cout << "CompactRange 0-e used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        start = "eeeeeeeeeeeeeeee";
        end =  "llllllllllllllll";
        s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());
        s = db->CompactRange(cr_option, &start, &end);
        assert(s.ok());
        cout << "CompactRange e-l used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        start = "llllllllllllllll";
        end =  "rrrrrrrrrrrrrrrr";
        s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());
        s = db->CompactRange(cr_option, &start, &end);
        assert(s.ok());
        cout << "CompactRange l-r used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
        start_time = getCurrentTime();

        start = "rrrrrrrrrrrrrrrr";
        end =  "zzzzzzzzzzzzzzzz";
        s = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), start, end);
        assert(s.ok());
        s = db->CompactRange(cr_option, &start, &end);
        assert(s.ok());
        cout << "CompactRange r-z used time: " << getCurrentTime() - start_time << " us" << endl;
        countIter(db);
    }

    string result;
    db->GetProperty("rocksdb.stats", &result);
    cout << "stats: " << result << endl;

    // 关闭
    db->Close();
    delete db;
}

// 测试CompactOnDeletionCollector
void testCompactOnDeletionCollector(const string db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;
    options.disable_auto_compactions = false;

    options.table_properties_collector_factories.emplace_back(
            rocksdb::NewCompactOnDeletionCollectorFactory(10000, 5000, 0.5));

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    int MAX_KEY_NUM = 1 * 1000 * 1000;
    for (int i = 0; i < MAX_KEY_NUM; ++i) {
        db->Put(WriteOptions(), std::to_string(i), genRandomString(100));
    }

    int MAX_DELETE_KEY_NUM = 900 * 1000;
    for (int i = 0; i < MAX_DELETE_KEY_NUM; ++i) {
        db->Delete(WriteOptions(), std::to_string(i));
    }

    // flush and compact
    db->Flush(FlushOptions());
    // db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();

    countIter(db);

    string result;
    db->GetProperty("rocksdb.stats", &result);
    cout << "stats: " << result << endl;

    cout << "internal_key_skipped_count: " << get_perf_context()->internal_key_skipped_count << endl;

    // 关闭
    db->Close();
    delete db;
}

// 自定义CompactionFilter
class DylanCompactionFilter: public CompactionFilter {
public:
    const char *Name() const override;
    Decision FilterV2(int level, const Slice& key, ValueType value_type,
                            const Slice& existing_value, std::string* new_value,
                            std::string* skip_until) const override;

};

const char* DylanCompactionFilter::Name() const {
    return "DylanCompactionFilter";
}

CompactionFilter::Decision DylanCompactionFilter::FilterV2(
    int level, const Slice& key, CompactionFilter::ValueType value_type,
    const Slice& existing_value, std::string* new_value, std::string* skip_until) const {
    cout << "level: " << level << " key: " << key.ToString()
        << " value_type: " << value_type << " existing_value: " << existing_value.ToString() << endl;

    return key.ToString() != string("5") ? CompactionFilter::Decision::kKeep: CompactionFilter::Decision::kRemove;
}


// 测试CompactionFilter
void testCompactionFilter(const string db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;
    // options.disable_auto_compactions = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    options.compaction_filter = new DylanCompactionFilter();

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    int MAX_KEY_NUM = 10;
    for (int i = 0; i < MAX_KEY_NUM; ++i) {
        db->Put(WriteOptions(), std::to_string(i), genRandomString(100));
    }

    int MAX_DELETE_KEY_NUM = 3;
    for (int i = 0; i < MAX_DELETE_KEY_NUM; ++i) {
        db->Delete(WriteOptions(), std::to_string(i));
    }

    // flush and compact
    db->Flush(FlushOptions());
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    countIter(db);

    cout << "compaction key drop user: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::COMPACTION_KEY_DROP_USER) << endl;

    // ROCKSDB_NAMESPACE::DestroyDB(db_path, options);
    // 关闭
    db->Close();
    delete db;
}

void testLevelBuild(const string db_path, const string data_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;
    // options.disable_auto_compactions = true;
    options.write_buffer_size = 4 * 1000 * 1000;
    options.max_write_buffer_number = 5;
    options.level0_file_num_compaction_trigger = 4;
    options.max_bytes_for_level_base = 64 * 1000 * 1000;
    options.max_bytes_for_level_multiplier = 4;
    options.target_file_size_base = 16 * 1000 * 1000;
    options.num_levels = 4;

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    // 写数据
    writeData(db, data_path);

    // 关闭
    db->Close();
    delete db;
}


// 测试testRepeatWrite
void testRepeatWrite(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism(10);
    options.max_background_jobs = 5;


    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "compact") {
        db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }

    // string result;
    // db->GetProperty("rocksdb.stats", &result);
    // cout << "stats: " << result << endl;

    // 关闭
    db->Close();
    delete db;
}

int main(int argc, char *argv[]) {
    // 开启perf
    // SetPerfLevel(ROCKSDB_NAMESPACE::PerfLevel::kEnableTime);

    // get_perf_context()->Reset();
    // get_perf_context()->EnablePerLevelPerfContext();

    // testCompaction03("/tmp/compact_test_01", "/root/work/rocksdb_dingo/examples/data/dataset_01", "delete_and_compact");

    // testDeleteFilesInRange("/tmp/compact_test_01", "/root/work/rocksdb_dingo/examples/data/dataset_01", "delete_file_in_range");

    // testCompactOnDeletionCollector("/tmp/compact_on_deletion");

    // testCompactionFilter("/tmp/compact_filter");

    // testLevelBuild("/tmp/level_build", "/root/work/rocksdb_dingo/examples/data/dataset_01");

    testRepeatWrite("/tmp/repeat_write_01", "/root/dengzihui/work/rocksdb_dingo/examples/data/dataset_02", "compact");
    // cout << get_perf_context()->ToString() << endl;

    return 0;
}