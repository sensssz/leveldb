CUSTOM_USR = /home/jiamin/usr
ROOT = ..
INCLUDE = -I$(CUSTOM_USR)/include -I$(ROOT)/include
LIBS = -lleveldb -lsnappy
LDFLAGS = -L$(CUSTOM_USR)/lib $(LIBS)
CXXFLAGS = -std=c++11 -c -O3 -g $(INCLUDE)
OBJS = glakv.o glakv_client.o glakv_server.o exponential_distribution.o

all: exec

test: exp_dist_test

exec: $(OBJS)
	$(CC) $(LDFLAGS) glakv_server.o -o glakv_server
	$(CC) $(LDFLAGS) glakv_client.o exponential_distribution.o -o glakv_client
	$(CC) $(LDFLAGS) glakv.o exponential_distribution.o -o glakv

glakv.o: glakv.cc config.h
	$(CC) $(CXXFLAGS) glakv.cc -o glakv.o

glakv_client.o: glakv_client.cc config.h
	$(CC) $(CXXFLAGS) glakv_client.cc -o glakv_client.o

glakv_server.o: glakv_server.cc config.h
	$(CC) $(CXXFLAGS) glakv_server.cc -o glakv_server.o

exp_dist_test: exp_dist_test.o exponential_distribution.o
	$(CC) $(LDFLAGS) exp_dist_test.o exponential_distribution.o -o exp_dist_test

exp_dist_test.o: exp_dist_test.cc
	$(CC) $(CXXFLAGS) exp_dist_test.cc -o exp_dist_test.o

exponential_distribution.o: exponential_distribution.cc exponential_distribution.h
	$(CC) $(CXXFLAGS) exponential_distribution.cc -o exponential_distribution.o

clean:
	rm $(OBJS)
