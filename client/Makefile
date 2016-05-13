CUSTOM_USR = /home/jiamin/usr
INCLUDE = -I$(CUSTOM_USR)/include
LIBS = -lleveldb -lsnappy
LDFLAGS = -L$(CUSTOM_USR)/lib $(LIBS)
CXXFLAGS = -std=c++11 -O3 -g $(INCLUDE)

all: main

main: config.h main.cc
    $(CC) $(CXXFLAGS) $(LDFLAGS) main.cc -o main
