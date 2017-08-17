#include <iostream>
#include <sstream>
#include <string>
#include <fstream>
#include <sstream>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"



void compact_range(leveldb::DB *db, const std::string& start, const std::string& end) {
    // TODO: check db
    leveldb::Slice cstart(start);
    leveldb::Slice cend(end);
    db->Write(leveldb::WriteOptions(), NULL);
    db->CompactRange(&cstart, &cend);
}


int main(int argc, char** argv)
{
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
    if (false == status.ok()) {
        std::cerr << "Unable to open/create test database './testdb'" << std::endl;
        std::cerr << status.ToString() << std::endl;
        return -1;
    }

    leveldb::WriteBatch batch;
    char *buffers[4] = {NULL, NULL, NULL, NULL};
    for (int i=1; i<5; ++i) {
        std::ostringstream keyStream;
        // std::ostringstream valueStream;
        keyStream << "Key" << i;
        // valueStream << "Test data value: " << i;
        // db->Put(writeOptions, keyStream.str(), valueStream.str());
        char *buffer = NULL;
        {
            std::stringstream fn;
            fn << i << ".txt";
            std::cout << fn.str() << std::endl;
            std::string fname = fn.str();
            std::cout << fname.c_str() << std::endl;
            std::ifstream in(fname.c_str(), std::ios::in|std::ios::binary|std::ios::ate);
            size_t sz = in.tellg();
            in.seekg(0, std::ios::beg);
            std::cout << "Size: "<< sz << std::endl;
            buffer = new char[sz];
            in.read(buffer, sz);
            in.close();
        }

        if (buffer) {
            buffers[i-1] = buffer;
            batch.Put(keyStream.str(), buffers[i-1]);
        }
    }
    db->Write(leveldb::WriteOptions(), &batch);

    // delete keys
    leveldb::WriteBatch bat;
    for (int i=1; i<5; i++) {
        std::ostringstream key;
        key << "Key" << i;
        bat.Delete(key.str());
    }
    db->Write(leveldb::WriteOptions(), &bat);

    std::string start("Key1");
    std::string end("Key4");
    // db->CompactRange(NULL, NULL);
    compact_range(db, start, end);
    // compact_range(db, start, end);
    
    // Iterate over each item in the database and print them
    // leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    // for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //     std::cout << it->key().ToString() << " : " << it->value().ToString() << std::endl;
    // }
    
    // if (false == it->status().ok()) {
    //     std::cerr << "An error was found during the scan" << std::endl;
    //     std::cerr << it->status().ToString() << std::endl;
    // }
    
    // delete it;
    delete db;  // Close the database
    for (int i=0; i<4; i++) {
        if (NULL != buffers[i]) {
            delete buffers[i];
        }
    }

    return 0;
}
