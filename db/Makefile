
CC=g++
CFLAGS=-std=c++11 -c -mcx16 -g -Wall -fgnu-tm 
INCLUDES=-I../ -I${METADB_INCLUDE} 
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< 

clean:
	$(RM) $(OBJECTS)

