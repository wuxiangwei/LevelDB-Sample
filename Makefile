# all: sample sample2
all: sample2

sample2: sample2.cc
	g++ -o $@ $< -std=c++11 -lpthread -lleveldb -lsnappy

sample: sample.cpp
	g++ -o $@ $<  -lleveldb -lpthread -lsnappy

clean:
	rm -f sample sample2 
	rm -rf store.db

db:
	rm -rf store.db
