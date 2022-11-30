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
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::NewBloomFilterPolicy;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using ROCKSDB_NAMESPACE::NewCappedPrefixTransform;
using ROCKSDB_NAMESPACE::get_perf_context;
using ROCKSDB_NAMESPACE::SetPerfLevel;


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

vector<string> genKeys(int nums) {
    vector<string> vec;
    for (int i = 0; i < nums; ++i) {
        vec.push_back(genRandomString(4));
    }

    return vec;
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

// 读取部分数据集
vector<string> readPartDataSet(const string filepath, int start_line, int end_line) {
    ifstream ifs;
    ifs.open(filepath, ios::in);

    vector<string> keys;
    char buf[128] = {0};
    int count = 0;
    while (ifs.getline(buf, sizeof(buf))) {
        ++count;
        if (count < start_line) {
            continue;
        } else if (count > end_line) {
            break;
        }
        vector<string> vec = splitStr(string(buf), ",");
        if (vec.size() < 2) {
            continue;
        }

        keys.push_back(vec[0]);
    }

    ifs.close();

    return keys;
}

// 得到当前时间-微秒
uint64_t getCurrentTime() {
    struct timeval val;
    gettimeofday(&val, nullptr);
    return val.tv_sec * 1000000 + val.tv_usec;
}

// 测试普通查询
void testNormal(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    // 开启perf
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();


    uint64_t start = getCurrentTime();

    if (action == "write") {
        // 写数据
        writeData(db, data_path);
    } else if (action == "read_point") {
        // 点查询
        vector<string> keys = readPartDataSet(data_path, 100000, 101000);
        for (auto key : keys) {
            string value;
            ReadOptions read_options = ReadOptions();
            read_options.auto_prefix_mode = true;
            s = db->Get(read_options, key, &value);
            cout << "value: " << value << endl;
            assert(s.ok());
        }
    } else if (action == "read_prefix") {
        ReadOptions read_options;
        // Slice upper_bound_slice = Slice("j8ozxt5jfsrlf2lo");
        // read_options.iterate_upper_bound = &upper_bound_slice;
        Iterator* it = db->NewIterator(read_options);
        int count = 0;
        // for (it->Seek("pg9zv"); it->Valid(); it->Next()) {
        //     ++count;
        //     cout << "key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
        // }

        for (it->Seek("pg9zv"); it->Valid() && it->key().starts_with("pg9zv"); it->Next()) {
            ++count;
            cout << "key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
        }

        delete it;
        cout << "count: " << count << endl;
    } else if (action == "read_prefix_batch") {
        vector<string> vec = genKeys(1000);
        ReadOptions read_options;
        read_options.auto_prefix_mode = false;

        int count = 0;
        for (auto key  : vec) {
            string upper = key;
            upper += "z";
            Slice upper_bound_slice = Slice(upper);
            read_options.iterate_upper_bound = &upper_bound_slice;

            Iterator* it = db->NewIterator(read_options);

            for (it->Seek(key); it->Valid() && it->key().starts_with(key); it->Next()) {
                ++count;
                cout << "prefix: " << key << " key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
            }

            delete it;
        }

        cout << "count: " << count << endl;
    }

    cout << "used time: " << getCurrentTime() - start << " us" << endl;

    cout << get_perf_context()->ToString() << endl;

    cout << "prefix checked: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_CHECKED) << endl;
    cout << "prefix useful: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_USEFUL) << endl;

    // 关闭
    db->Close();
    delete db;
}


// 测试前缀查询
void testPrefix(const string db_path, const string data_path, const string action) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // 设置布隆过滤器
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = true;
    // table_options.cache_index_and_filter_blocks = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.prefix_extractor.reset(NewCappedPrefixTransform(2));

    // 打开DB
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    // 开启perf
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();

    uint64_t start = getCurrentTime();

    if (action == "write") {
        // 写数据
        writeData(db, data_path);

    } else if (action == "read_point") {
        // 点查询
        vector<string> keys = readPartDataSet(data_path, 100000, 100100);
        for (auto key : keys) {
            string value;
            ReadOptions read_options = ReadOptions();
            read_options.auto_prefix_mode = true;
            s = db->Get(read_options, key, &value);
            cout << "value: " << value << endl;
            assert(s.ok());
        }

    } else if (action == "read_prefix") {
        // 前缀查询
        ReadOptions read_options;
        read_options.auto_prefix_mode = true;
        read_options.prefix_same_as_start = true;
        Slice upper_bound_slice = Slice(";;z");
        read_options.iterate_upper_bound = &upper_bound_slice;
        // Slice lower_bound_slice = Slice("pg9zv");
        // read_options.iterate_lower_bound = &lower_bound_slice;
        Iterator* it = db->NewIterator(read_options);
        int count = 0;
        for (it->Seek(";;"); it->Valid() && it->key().starts_with(";;"); it->Next()) {
            ++count;
            // cout << "key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
        }

        delete it;
        cout << "count: " << count << endl;
    } else if (action == "read_prefix_batch") {
        vector<string> vec = genKeys(1000);

        ReadOptions read_options;
        read_options.auto_prefix_mode = true;
        read_options.prefix_same_as_start = true;
        int count = 0;
        for (auto key  : vec) {
            string upper = key;
            upper += "z";
            Slice upper_bound_slice = Slice(upper);
            read_options.iterate_upper_bound = &upper_bound_slice;

            Iterator* it = db->NewIterator(read_options);

            for (it->Seek(key); it->Valid() && it->key().starts_with(key); it->Next()) {
                ++count;
                cout << "prefix: " << key << " key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
            }

            delete it;
        }

        
        cout << "count: " << count << endl;
    }

    cout << "used time: " << getCurrentTime() - start << " us" << endl;

    cout << get_perf_context()->ToString() << endl;

    // string result;
    // db->GetProperty("rocksdb.stats", &result);
    // cout << "stats: " << result << endl;

    cout << "prefix checked: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_CHECKED) << endl;
    cout << "prefix useful: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_USEFUL) << endl;

    // 关闭
    db->Close();
    delete db;
}

int CountIter(std::unique_ptr<Iterator>& it, const Slice& key) {
  int count = 0;
  for (it->Seek(key); it->Valid(); it->Next()) {
    cout << "prefix: " << key.ToString() << " key: " << it->key().ToString() << " value: " << it->value().ToString() << endl;
    count++;
  }
  return count;
}

// 
// 配置: 
//     filter_policy: NewBloomFilterPolicy(10, false)
//     prefix_extractor: NewCappedPrefixTransform(4)
//     auto_prefix_mode: true
//     prefix_same_as_start: true
//     iterate_upper_bound: 
void testPrefixExample01(const string db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // 设置布隆过滤器
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = true;
    table_options.cache_index_and_filter_blocks = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.prefix_extractor.reset(NewCappedPrefixTransform(4));

    // 打开DB
    rmdir(db_path.c_str());
    sleep(2);
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    // 开启perf
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();

    db->Put(WriteOptions(), "aaaa1000", "value01");
    db->Put(WriteOptions(), "abbb10001", "value02");
    db->Put(WriteOptions(), "aaaa10002", "value03");
    db->Put(WriteOptions(), "abbb100043", "value04");

    db->Flush(FlushOptions());

    // ReadOptions read_options;
    // read_options.auto_prefix_mode = true;
    // // read_options.prefix_same_as_start = true;
    // Slice upper_bound_slice = Slice("aaab");
    // read_options.iterate_upper_bound = &upper_bound_slice;

    // std::unique_ptr<Iterator> iter_tmp(db->NewIterator(read_options));
    // CountIter(iter_tmp, "aaaa1000"); // expect [aaaa1000, aaaa10002]

    string value;
    ReadOptions read_options = ReadOptions();
    s = db->Get(read_options, "abbb10001", &value);

    cout << get_perf_context()->ToString() << endl;

    cout << "prefix checked: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_CHECKED) << endl;
    cout << "prefix useful: " << options.statistics->getAndResetTickerCount(ROCKSDB_NAMESPACE::BLOOM_FILTER_PREFIX_USEFUL) << endl;


    sleep(10);
    // 关闭
    db->Close();
    delete db;
}

void testSlice(const string db_path) {
    DB* db;
    Options options;
    options.create_if_missing = true;

    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());
    
    db->Put(WriteOptions(), "aaaa", "value01");
    db->Put(WriteOptions(), "aaaab", "value02");
    db->Put(WriteOptions(), "aaab", "value03");

    char buf[10] = {0};
    memcpy(buf, (void*)"aaaa", 4);
    Slice endKey(buf, 5);

    db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), nullptr, endKey);

    // string value;
    // ReadOptions read_options = ReadOptions();
    // s = db->Get(read_options, "aaaa", &value);

    // cout << "value: " << value << endl;

    Iterator* it = db->NewIterator(ReadOptions());

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << " value: " << it->value().ToString() << endl;
    }


    // 关闭
    db->Close();
    delete db;
}


int main(int argc, char *argv[]) {
    SetPerfLevel(ROCKSDB_NAMESPACE::PerfLevel::kEnableTime);

    // testNormal("/tmp/prefix_test_01", "/root/work/rocksdb/examples/data/dataset_01", "read_prefix_batch");

    // testPrefix("/tmp/prefix_2_test", "/root/work/rocksdb_dingo/examples/data/dataset_01", "read_prefix");

    // testPrefixExample01("/tmp/prefix_test_example_01");

    testSlice("/tmp/test_slice");

    return 0;
}