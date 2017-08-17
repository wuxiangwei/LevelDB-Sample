#include <iostream>
#include <sstream>
#include <string>
#include "leveldb/db.h"


int main(int argc, char** argv)
{
    // Set up database connection information and open database
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
    if (false == status.ok()) {
        std::cerr << "Unable to open/create test database './testdb'" << std::endl;
        std::cerr << status.ToString() << std::endl;
        return -1;
    }
    
    // Add 256 values to the database
    leveldb::WriteOptions writeOptions;
    for (unsigned int i = 0; i < 256; ++i) {
        std::ostringstream keyStream;
        std::ostringstream valueStream;

        keyStream << "Key" << i;
        valueStream << "Test data value: " << i;
        db->Put(writeOptions, keyStream.str(), valueStream.str());
    }
    
    // Iterate over each item in the database and print them
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << " : " << it->value().ToString() << std::endl;
    }
    
    if (false == it->status().ok()) {
        std::cerr << "An error was found during the scan" << std::endl;
        std::cerr << it->status().ToString() << std::endl;
    }
    
    delete it;
    delete db;  // Close the database

    return 0;
}
