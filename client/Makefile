CUSTOM_USR = /home/jiamin/usr
ROOT = ../
INCLUDE = -I$(CUSTOM_USR)/include -I$(ROOT)/include
LIBS = -lleveldb -lsnappy
LDFLAGS = -L$(CUSTOM_USR)/lib $(LIBS)
CXXFLAGS = -std=c++11 -O3 -g $(INCLUDE)

all: main

main: config.h glakv.cc
	$(CC) $(CXXFLAGS) $(LDFLAGS) glakv.cc -o glakv
