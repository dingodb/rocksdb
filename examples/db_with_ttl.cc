#include <cstdio>
#include <string>
#include <iostream>
#include <unistd.h>
#include <time.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/db_ttl.h"

using ROCKSDB_NAMESPACE::DBWithTTL;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using ROCKSDB_NAMESPACE::DBOptions; // zhangjie add
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;

std::string kDBPath = "/tmp/rocksdb_with_ttl";

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif
constexpr bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

const int TIME_LENGTH = sizeof(int32_t);

const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

void EncodeFixed32(char* buf, uint32_t value) {
  if (kLittleEndian) {
    std::cout << "litter endian." << std::endl;
    memcpy(buf, &value, sizeof(value));
  } else {
    std::cout << "not litter endian." << std::endl;
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

uint32_t DecodeFixed32(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cout << "usage: ./" << argv[0] << " <count> <index>" << std::endl;
    exit(-1);
  }
  DBWithTTL* db;
  Options options;
  options.create_if_missing = true;

  // open DB
  //Status s = DBWithTTL::Open(options, kDBPath, &db, 10);
  Status s = DBWithTTL::Open(options, kDBPath, &db, 10, false, true);
  assert(s.ok());

  int count = std::atoi(argv[1]);
  if (count <= 0) {
    std::cout << "count invalid, count: " << count << std::endl;
    exit(-1);
  }
  char* index_str = argv[2];
  time_t curtime = time(nullptr);
  char ts_str[TIME_LENGTH];
  bzero(ts_str, TIME_LENGTH);
  printf("current time: %d.\n", curtime);
  EncodeFixed32(ts_str, (int32_t)curtime);
  std::cout << "encode done!" << std::endl;
  std::cout << "decode value: " << DecodeFixed32(ts_str) << std::endl;

  for (int i = 0; i <= count; i++) {
    // Put key-value
    char k[32];
    bzero(k, 32);
    char value[64];
    bzero(value, 64);
    sprintf(k, "key_%08d", i);
    sprintf(value, "value_%08d_%s", i, index_str);
    std::string valueWithTs = std::string(value);
    valueWithTs.append(ts_str, TIME_LENGTH);
    std::cout << "put => key: " << k << ", valueWithTs length: " << valueWithTs.length() << ", value length: " << strlen(value) << std::endl;
    s = db->Put(WriteOptions(), k, valueWithTs);
    assert(s.ok());
    if (i % 1000 == 0) {
      std::cout << i << std::endl;
    }
  }
  printf("put done!\n");

  std::string value;
  for (int i = 0; i <= count; i++) {
    char k[32] = {0};
      sprintf(k, "key_%08d", i);
      s = db->Get(ReadOptions(), k, &value);
      if (s.ok()) {
        std::cout << "get => key: " << k << ", value length: " << value.length() << ", value: " << value << std::endl;
      } else {
        std::cout << "get =>key: " << k << ", status not OK: " << s.ToString() << std::endl;
      }
  }

  fflush(stdout);

  delete db;
  return 0;
}
