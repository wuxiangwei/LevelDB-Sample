#include <iostream>
#include <thread>
#include <mutex>
#include <queue>
#include <utility>
#include <atomic>
#include <sstream>


uint64_t g_first_committed;
uint64_t g_last_committed;
uint64_t g_paxos_min; // 50
uint64_t g_paxos_trim_min; // 250 
uint64_t g_paxos_trim_max; // 500
std::mutex g_paxos_mutex;

typedef std::string KeyType;
std::queue<std::pair<KeyType, KeyType>> g_compact_queue;
std::mutex g_compact_mutex;

std::atomic<bool> g_stop_compact;
std::mutex g_print_mutex;

// called by main thread
void init() {
    g_stop_compact = false;
    g_paxos_min = 5;
    g_paxos_trim_min = 25;
    g_paxos_trim_max = 50;
    g_first_committed = g_last_committed = 0;
    // TODO: Open leveldb
}


std::string get_key_by_index(uint64_t index) {
    std::ostringstream key;
    key << "paxos_" << index;

    return key.str();
}

void write_entry() {
    for (uint64_t i=0; i<100; i++) {
        g_paxos_mutex.lock();
        // add new entry
        g_last_committed++;

        // check trim
        if (g_first_committed + g_paxos_min + g_paxos_trim_min < g_last_committed) {
            uint64_t trim_num = g_last_committed - g_paxos_min - g_first_committed;
            if (trim_num > g_paxos_trim_max) {
                trim_num = g_paxos_trim_max;
            }
            
            uint64_t ed_key_index = g_first_committed + trim_num;
            KeyType ed_key = get_key_by_index(ed_key_index);
            KeyType be_key = get_key_by_index(g_first_committed);

            g_compact_mutex.lock();
            g_compact_queue.push(std::pair<KeyType, KeyType>(be_key, ed_key));
            g_compact_mutex.unlock();

            g_first_committed += trim_num;
        }

        g_print_mutex.lock();
        std::cout << "[Write]" 
            << " first_committed: " << g_first_committed
            << " last_committed: " << g_last_committed << std::endl;
        g_print_mutex.unlock();
        g_paxos_mutex.unlock();
        
        // TODO: Write entry to db
    }
}


void compact_entry() {
    while (!g_stop_compact) {
        std::pair<KeyType, KeyType> e;
        g_compact_mutex.lock();
        if (!g_compact_queue.empty()) {
            e = g_compact_queue.front();
            g_print_mutex.lock();
            std::cout << "[Compact] " << e.first << " ~ " << e.second << std::endl;
            g_print_mutex.unlock();
            g_compact_queue.pop();
        }
        g_compact_mutex.unlock();
    }
}

int main(int argc, char* argv[]) {
    
    init();

    std::thread th1(write_entry);
    std::thread th2(compact_entry);

    th1.join();
    g_stop_compact = true;
    th2.join();

    return 0;
}
