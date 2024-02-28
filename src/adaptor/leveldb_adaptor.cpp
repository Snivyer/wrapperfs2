#include "adaptor/leveldb_adaptor.h"

namespace wrapperfs {

LevelDBAdaptor::LevelDBAdaptor(const std::string &database_name) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, database_name, &db_);
    if (!status.ok()) {
        spdlog::error("leveldbadaptor database_name - {} :leveldb can't open failed", database_name);
        exit(1);
    }
}

LevelDBAdaptor::~LevelDBAdaptor() {
    delete db_;
}

bool LevelDBAdaptor::Insert(const std::string &key, const std::string &value) {
    leveldb::WriteOptions write_options;
    write_options.sync = true;
    leveldb::Status status = db_->Put(write_options, key, value);
    return status.ok();
}

bool LevelDBAdaptor::GetValue(const std::string &key, std::string &value) {
    leveldb::ReadOptions read_options;
    leveldb::Status status = db_->Get(read_options, key, &value);
    return status.ok();
}

bool LevelDBAdaptor::Remove(const std::string &key) {
    leveldb::WriteOptions write_options;
    write_options.sync = true;
    leveldb::Status status = db_->Delete(write_options, key);
    return status.ok();
}


bool LevelDBAdaptor::GetRange(const std::string &start_key, const std::string &end_key, std::vector<std::pair<std::string, std::string>> &key_value_pair_list) {
    key_value_pair_list.clear();
    leveldb::ReadOptions read_options;
    leveldb::Iterator* it = db_->NewIterator(read_options);
    for (it->Seek(start_key); it->Valid() && it->key().ToString() < end_key; it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        key_value_pair_list.push_back(std::make_pair(key, value));
    }
    leveldb::Status status = it->status();
    delete it;
    return status.ok();
}

}