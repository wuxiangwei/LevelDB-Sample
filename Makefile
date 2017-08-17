all: sample

sample: sample.cpp
	g++ -o $@ $<  -lleveldb -lpthread -lsnappy

clean:
	rm -f sample
	rm -rf testdb

db:
	rm -rf testdb
