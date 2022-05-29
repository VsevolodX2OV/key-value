// Copyright by Enigma

#include <database.hpp>
#include <iostream>


void make_inp_BD(const std::string& directory) {
  const unsigned int NUMBER_OF_COLUMNS = 3;
  const unsigned int NUMBER_OF_VALUES = 5;

  try {
    rocksdb::Options options;

    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;

    rocksdb::Status status = rocksdb::DB::Open(options, directory, &db);

    if (!status.ok()) throw std::runtime_error{"DB::Open failed"};


    std::vector<std::string> column_family;


    column_family.reserve(NUMBER_OF_COLUMNS);

    for (unsigned int i = 0; i < NUMBER_OF_COLUMNS; ++i) {
      column_family.emplace_back("ColumnFamily_" + std::to_string(i + 1));
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = db->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                      column_family, &handles);
    if (!status.ok()) throw std::runtime_error{"CreateColumnFamilies failed"};


    std::string key;
    std::string value;
    for (unsigned int i = 0; i < NUMBER_OF_COLUMNS; ++i) {
      for (unsigned int j = 0; j < NUMBER_OF_VALUES; ++j) {
        key = "key-" + std::to_string((i * NUMBER_OF_VALUES) + j);
        value = "value-" + std::to_string(std::rand() % 100);
        status = db->Put(rocksdb::WriteOptions(), handles[i],
                         rocksdb::Slice(key), rocksdb::Slice(value));
        if (!status.ok())
          throw std::runtime_error{"Putting [" + std::to_string(i + 1) + "][" +
                                   std::to_string(j) + "] failed"};

        BOOST_LOG_TRIVIAL(info) << "Added [" << key << "]:[" << value
                                << "] -- [" << i + 1 << " family column ]"
                                <<" -- [ FIRST DATA BASE ]";
      }
    }

    for (auto& x : handles) {
      status = db->DestroyColumnFamilyHandle(x);
      if (!status.ok()) throw std::runtime_error{"DestroyColumnFamily failed"};
    }

    delete db;
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}

My_BD::My_BD(std::string& input_dir,
             std::string& output_dir,
             size_t number_of_threads)
    : ProdQueue_(),
      ConsQueue_(),
      input_(input_dir),
      output_(output_dir),
      pool_(number_of_threads) {

  rocksdb::Status s{};
  std::vector<std::string> names;

  std::vector<rocksdb::ColumnFamilyDescriptor> desc;
  try {
    s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), input_, &names);
    if (!s.ok()) throw std::runtime_error("ListColumnFamilies is failed");


    desc.reserve(names.size());

    for (auto& x : names) {
      desc.emplace_back(x, rocksdb::ColumnFamilyOptions());
    }


    s = rocksdb::DB::OpenForReadOnly(rocksdb::DBOptions(), input_, desc,
                                     &fromHandles_, &inpBD_);
    if (!s.ok())
      throw std::runtime_error("OpenForReadOnly of input DB is failed");

    names.erase(names.begin());

    rocksdb::Options options;

    options.create_if_missing = true;

    s = rocksdb::DB::Open(options, output_, &outputBD_);
    if (!s.ok()) throw std::runtime_error("Open of output DB is failed");

    outputBD_->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(), names,
                                    &outHandles_);

    outHandles_.insert(outHandles_.begin(), outputBD_->DefaultColumnFamily());
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}


void My_BD::parse_inp_BD() {
  std::vector<rocksdb::Iterator*> iterators;
  rocksdb::Iterator* it;

  for (size_t i = 0; i < fromHandles_.size(); ++i) {
    it = inpBD_->NewIterator(rocksdb::ReadOptions(), fromHandles_[i]);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ProdQueue_.push({i, it->key().ToString(), it->value().ToString()});
    }
    iterators.emplace_back(it);
    it = nullptr;
  }

  for (auto& x : iterators) {
    delete x;
  }

  ParseFlag_ = true;
}


My_BD::~My_BD() {
  try {
    rocksdb::Status s;
    if (!fromHandles_.empty() && inpBD_ != nullptr) {
      for (auto& x : fromHandles_) {
        s = inpBD_->DestroyColumnFamilyHandle(x);
        if (!s.ok()) {
          throw std::runtime_error("Destroy From Handle failed in destructor");
        }
      }
      fromHandles_.clear();
      s = inpBD_->Close();
      if (!s.ok()) {
        throw std::runtime_error("Closing of fromDB in destructor");
      }
      delete inpBD_;
    }

    if (!outHandles_.empty() && outputBD_ != nullptr) {
      for (auto& x : outHandles_) {
        s = outputBD_->DestroyColumnFamilyHandle(x);
        if (!s.ok()) {
          throw std::runtime_error(
              "Destroy Output Handle failed in destructor");
        }
      }
      outHandles_.clear();
    }
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}

void My_BD::write_val_to_BD(Entry &&Key_Hash) {
  try {
    rocksdb::Status s = outputBD_->Put(rocksdb::WriteOptions(),
                                       outHandles_[Key_Hash.Handle],
                                       Key_Hash.Key, Key_Hash.Value);
    BOOST_LOG_TRIVIAL(info)
        <<"[" << Key_Hash.Key << "] " << " [" << Key_Hash.Value << "] "
        << " [-NEW DATA BASE-]";
    if (!s.ok()) {
      throw std::runtime_error("Writing in output DB is failed");
    }
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}

std::string calc_hash(const std::string& key, const std::string& value) {
  return picosha2::hash256_hex_string(std::string(key + value));
}

void My_BD::make_cons_queue(Entry& en) {
  ConsQueue_.push({en.Handle, en.Key, calc_hash(en.Key, en.Value)});
}

void My_BD::make_cons_pool() {
  Entry item;

  while (!ParseFlag_ || !ProdQueue_.empty()) {
    if (ProdQueue_.pop(item)) {
      pool_.enqueue([this](Entry x) { make_cons_queue(x); }, item);
    }
  }
  HashFlag_ = true;
}

void My_BD::write_new_BD() {
  Entry item;

  while (!ConsQueue_.empty() || !HashFlag_) {
    if (ConsQueue_.pop(item)) {
      write_val_to_BD(std::move(item));
    }
  }
  WriteFlag_ = true;
}

void My_BD::start_process() {
  std::thread producer([this]() { parse_inp_BD(); });

  std::thread consumer([this]() { write_new_BD(); });

  producer.join();
  make_cons_pool();
  consumer.join();

  while (!HashFlag_ || !ParseFlag_ || !WriteFlag_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}
