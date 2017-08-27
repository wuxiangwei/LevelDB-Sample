#include <iostream>
#include <thread>
#include <mutex>
#include <queue>
#include <utility>
#include <atomic>
#include <sstream>
#include <assert.h>
#include <chrono>
#include <condition_variable>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"


leveldb::DB* g_db;

uint64_t g_first_committed;
uint64_t g_last_committed;
uint64_t g_paxos_min; // 50
uint64_t g_paxos_trim_min; // 250 
uint64_t g_paxos_trim_max; // 500

typedef std::string KeyType;
std::queue<std::pair<KeyType, KeyType>> g_compact_queue;
std::mutex g_compact_mutex;
std::condition_variable g_compact_cv;

std::atomic<bool> g_stop_compact;
std::mutex g_print_mutex;

size_t g_max_entries;

// called by main thread
void init() {
    g_stop_compact = false;
    g_paxos_min = 50;
    g_paxos_trim_min = 250;
    g_paxos_trim_max = 500;
    g_first_committed = g_last_committed = 0;
    
    g_max_entries = 1000000; // g_max_entries * BATCH_SIZE

    // Open leveldb
    leveldb::Options opt;
    opt.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(opt, "./store.db", &g_db);
    assert(s.ok());
}


void deinit() {
    if (g_db) {
        delete g_db;
        g_db = NULL;
    }

    std::cout << "[Main] first_committed: " << g_first_committed 
        << " last_committed: " << g_last_committed << std::endl;

    std::cout << "[Main] compact queue: " << std::endl;
    while (!g_compact_queue.empty()) {
        std::pair<KeyType, KeyType> e = g_compact_queue.front();
        std::cout << e.first << " ~ " << e.second << std::endl;
        g_compact_queue.pop();
    }
}


std::string get_key_by_index(uint64_t index) {
    std::ostringstream key;
    key << "paxos_" << index;

    return key.str();
}


const size_t VALUE_SIZE = 2 * 1024 * 1024; // 2MB
char* get_value_by_index(uint64_t index=0) {
    char *buffer = new char[VALUE_SIZE];

    for (size_t i=0; i<VALUE_SIZE; i++) {
        buffer[i] = 'x';
    }

    return buffer;
}



const size_t BATCH_SIZE = 4;
void write_entry() {
    for (uint64_t i=0; i<g_max_entries; i++) {
        leveldb::WriteBatch bat;
        char *buffers[BATCH_SIZE]; 
        for (size_t j=0; j<BATCH_SIZE; j++) {
            uint64_t key_ind = i * BATCH_SIZE + j;
            KeyType key = get_key_by_index(key_ind);
            buffers[j] = get_value_by_index(i*BATCH_SIZE+j);
            bat.Put(key, buffers[j]);
        }

        // add new entry
        g_last_committed += BATCH_SIZE;

        // check trim
        if (g_first_committed + g_paxos_min + g_paxos_trim_min < g_last_committed) {
            uint64_t trim_num = g_last_committed - g_paxos_min - g_first_committed;
            if (trim_num > g_paxos_trim_max) {
                trim_num = g_paxos_trim_max;
            }
            
            uint64_t ed_key_index = g_first_committed + trim_num;
            KeyType be_key = get_key_by_index(g_first_committed);
            KeyType ed_key = get_key_by_index(ed_key_index);

            for (size_t k=g_first_committed; k<ed_key_index; k++) {
                KeyType delete_key = get_key_by_index(k);
                bat.Delete(delete_key);
            }

            std::unique_lock<std::mutex> compact_lock(g_compact_mutex);
            g_compact_queue.push(std::pair<KeyType, KeyType>(be_key, ed_key));
            g_compact_cv.notify_all();

            g_first_committed += trim_num;
        }

        if (0) {
            std::unique_lock<std::mutex> loc(g_print_mutex);
            std::cout << "[Write]" 
                << " first_committed: " << g_first_committed
                << " last_committed: " << g_last_committed << std::endl;
        }
        
        // Write entries to db
        g_db->Write(leveldb::WriteOptions(), &bat);

        // Release value buffer
        for (size_t j=0; j<BATCH_SIZE; j++) {
            delete[] buffers[j];
        }

        // std::this_thread::sleep_for(std::chrono::seconds(1)); // sleep 1 seconds
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // sleep 1 seconds
    }
}


void compact_entry() {
    while (!g_stop_compact) {
        std::pair<KeyType, KeyType> e("", "");
        {
            std::unique_lock<std::mutex> compact_lock(g_compact_mutex);
            while (g_compact_queue.empty()) {
                if (std::cv_status::timeout == g_compact_cv.wait_for(compact_lock, std::chrono::seconds(10))) {
                    break;
                }
            }

            if (!g_compact_queue.empty()) {
                e = g_compact_queue.front();
                g_compact_queue.pop();
            }

            std::unique_lock<std::mutex> loc(g_print_mutex); 
            std::cout << "[Compact] queue_len: " << g_compact_queue.size() << std::endl;
        }
        
        if (!(e.first == "" && e.second == "")) {
            leveldb::Slice slc_begin(e.first);
            leveldb::Slice slc_end(e.second);
            g_db->CompactRange(&slc_begin, &slc_end);

            std::unique_lock<std::mutex> loc(g_print_mutex);
            std::cout << "[Compact] compacting " << e.first << " ~ " << e.second << std::endl;
        }
    }
}



int main(int argc, char* argv[]) {
    
    init();

    std::thread th1(write_entry);
    std::thread th2(compact_entry);

    th1.join();
    g_stop_compact = true;
    th2.join();

    deinit();

    return 0;
}
