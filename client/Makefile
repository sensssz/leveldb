CUSTOM_USR = /home/jiamin/usr
ROOT = ..
INCLUDE = -I$(CUSTOM_USR)/include -I$(ROOT)/include
LIBS = -lleveldb -lsnappy
LDFLAGS = -L$(CUSTOM_USR)/lib $(LIBS)
CXXFLAGS = -std=c++11 -c -O3 -g $(INCLUDE)
OBJS = glakv.o exponential_distribution.o

all: exec

exec: $(OBJS)
    $(CC) $(LDFLAGS) $(OBJS) -o glakv

glakv.o: glakv.cc config.h
	$(CC) $(CXXFLAGS) glakv.cc -o glakv.o

exponential_distribution.o: exponential_distribution.cc exponential_distribution.h
	$(CC) $(CXXFLAGS) exponential_distribution.cc -o exponential_distribution.o
