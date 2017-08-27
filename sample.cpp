#include <iostream>
#include <sstream>
#include <string>
#include <fstream>
#include <sstream>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"


// compact
void compact_range(leveldb::DB *db, 
    const std::string& start, const std::string& end, bool write_null=false) {
    leveldb::Slice cstart(start);
    leveldb::Slice cend(end);
    if (write_null) {
        db->Write(leveldb::WriteOptions(), NULL);
        std::cout << "write NULL" << std::endl;
    }
    db->CompactRange(&cstart, &cend);
    std::cout << "compact_range(" << start << ", " << end << ", " << write_null << ")" << std::endl;
}


char* gen_value(const std::string& key) {
    std::string fn = "./testdata/" + key;
    std::ifstream in(fn.c_str(), std::ios::in|std::ios::binary|std::ios::ate);
    size_t sz = in.tellg();
    in.seekg(0, std::ios::beg);
    std::cout << "Size: "<< sz << std::endl;
    char* buffer = new char[sz];
    in.read(buffer, sz);
    in.close();
    return buffer;
}


int main(int argc, char** argv)
{
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, "./store.db", &db);
    if (!status.ok()) {
        std::cerr << "Unable to open/create test database './store.db'" << std::endl;
        std::cerr << status.ToString() << std::endl;
        return -1;
    }

    const unsigned KEY_NUM = 4;
    char *buffers[KEY_NUM];
    leveldb::WriteBatch batch;

    // write first
    for (int i=0; i<KEY_NUM; i++) {
        std::ostringstream key;
        key << i+1 << ".txt";
        buffers[i] = gen_value(key.str());
        batch.Put(key.str(), buffers[i]);
    }
    db->Write(leveldb::WriteOptions(), &batch);

    // delete then
    batch.Clear();
    for (int i=0; i<KEY_NUM; i++) {
        std::ostringstream key;
        key << i+1 << ".txt";
        batch.Delete(key.str());
    }
    db->Write(leveldb::WriteOptions(), &batch);

    std::string start("1.txt");
    std::string end("4.txt");
    // case1: Big
    // db->CompactRange(NULL, NULL);
    // case2: OK
    // compact_range(db, start, end, true);
    // case3: Big
    compact_range(db, start, end);
    // case4: compact twice, OK
    // compact_range(db, start, end);
    // compact_range(db, start, end);
    
    delete db;  // Close the database
    for (int i=0; i<KEY_NUM; i++) {
        if (NULL != buffers[i]) {
            delete buffers[i];
        }
    }

    return 0;
}
