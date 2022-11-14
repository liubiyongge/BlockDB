
CXX = g++
BUILD_MODE = debug

CXXFLAGS = -std=c++11 -Wall -lrt
CXXFLAGS += -I src/
CXXFLAGS += -L /home/wxl/Block_Compaction/leveldb-1.34/out-static/
CXXFLAGS += -lleveldb -pthread -laio -lsnappy  -lz -lbz2 -lzstd -llz4 -ldl

ifeq ($(BUILD_MODE), debug)
	CXXFLAGS += -g
else ifeq ($(BUILD_MODE), run)
	CXXFLAGS += -O2 -DNDEBUG
else
	$(error Build mode $(BUILD_MODE) not supported)
endif

all: simple

simple: test_blockdb.o src/simple.cc
	$(CXX)  src/simple.cc obj/test_blockdb.o -o bin/simple $(CXXFLAGS)

test_blockdb.o: src/test_blockdb.cc
	$(CXX) -o obj/test_blockdb.o -c src/test_blockdb.cc $(CXXFLAGS)

clean:
	rm -rf bin/* obj/*
