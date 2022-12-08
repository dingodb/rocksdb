#include <unistd.h>
#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include <sys/time.h>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "gflags/gflags.h"

#define VALUE_LEN 500 

using namespace google;

DEFINE_int64(time,1200,"read threads' ratio");
DEFINE_int64(thread_num,64,"total threads ");
DEFINE_string(db_dir, "srd_delete", "db's dir is delete");
DEFINE_bool(use_fullcompaction, false, "use full compaction to db");
DEFINE_int64(num_keys, 1000000,"write keys' num");
DEFINE_int64(wait_compaction,10,"wait");
DEFINE_bool(traverse, true, "use traverse");
DEFINE_bool(use_delete_collector, false, "use traverse");

std::atomic<long> g_op_W;

rocksdb::DB* src_db;
rocksdb::Options options;
std::mt19937_64 generator_; // 生成伪随机数

// 返回微秒
inline int64_t now() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return static_cast<int64_t>(t.tv_sec)*1000000 + t.tv_usec;
}

// 打开rocksdb
void OpenDB() {
  options.create_if_missing = true;
  options.compression = rocksdb::kNoCompression;
  options.disable_auto_compactions = FLAGS_use_fullcompaction;
  
  if(FLAGS_use_delete_collector) {
    options.table_properties_collector_factories.emplace_back(rocksdb::NewCompactOnDeletionCollectorFactory(1000000, 1000));
  }
  std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(10737418240);

  rocksdb::BlockBasedTableOptions bbto;
  bbto.whole_key_filtering = true;
  bbto.cache_index_and_filter_blocks = true;
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(16,false));
  bbto.block_cache = cache;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

  options.max_background_compactions = 32;
  auto s = rocksdb::DB::Open(options, FLAGS_db_dir, &src_db);
  std::cout << "open src_db" <<  s.ToString() << std::endl;

}

// 配置手动执行full compaction
void FullCompaction() {
  rocksdb::Slice begin("");

  rocksdb::Iterator *it = src_db->NewIterator(rocksdb::ReadOptions());
  it->SeekToLast();
  std::string end_str = it->key().ToString();
  delete it;

  rocksdb::Slice end(end_str);
  auto s = src_db->CompactRange(rocksdb::CompactRangeOptions(), &begin, &end);
  if (!s.ok()) {
    std::cout << "CompactRange failed " << s.ToString() << std::endl;
  }
}

// 写入 + 删除
void DOWrite(int num) {
  int count = FLAGS_num_keys;
  std::string value(VALUE_LEN, 'x'); 
  while(count > 0) {
    src_db -> Put(rocksdb::WriteOptions(), std::string("test_compaction_129_")+std::to_string(count),value);
    ++ g_op_W;
    count --;
  }

  // 删除90% 的key
  std::cout << "Begin delete "<< std::endl;
  for (int i = 0;i < FLAGS_num_keys - FLAGS_num_keys / 10; ++i) {
    src_db->Delete(rocksdb::WriteOptions(), std::string("test_compaction_129_")+std::to_string(i));
  }
  src_db->SetOptions(src_db->DefaultColumnFamily(),{{"diable_auto_compactions","true"}});
  std::cout << "End delete \n";

  // 开启full compaction
  if(FLAGS_use_fullcompaction) {
    FullCompaction();
    sleep(FLAGS_wait_compaction);
  }
}

void Traverse() {
  // open perf 
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
  rocksdb::get_perf_context()->Reset();
  rocksdb::get_iostats_context()->Reset();

  rocksdb::Iterator *it = src_db->NewIterator(rocksdb::ReadOptions());
  it->Seek("");
  
  std::cout << "Traverse begin :" << std::endl;
  int64_t ts = now();
  int seek_count = 0;
  for(; it->Valid()&& seek_count<500;  it->Next() ) {
    std::cout << it->key().ToString() << std::endl; 
    seek_count++;
  }

  delete it;
  
  // close perf，查看internal_delete_skipped_count指标
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  std::cout << "Traverse use time :" << now() - ts << std::endl;
  std::cout << "internal_delete_skipped_count "  
            << rocksdb::get_perf_context()->internal_delete_skipped_count
            << std::endl;
}

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc,&argv, true);

  OpenDB();

  for(int i = 0;i < FLAGS_thread_num; i++) {
    new std::thread(DOWrite,i);
  }


  long last_opn_W = 0;
  int count = FLAGS_time;
  while(count > 0) {
    sleep(1);
    long nopn_W = g_op_W;
    
    std::cout << "write_speed : " << nopn_W - last_opn_W << std::endl;
    last_opn_W = nopn_W;
    count --;
  }

  if(FLAGS_traverse){
    Traverse();
  }
  return 0;
}
