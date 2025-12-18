CC=gcc
CFLAGS=-Wall -Wextra -O2

all: client nameserver storageserver

client: client.c common.h
	$(CC) $(CFLAGS) client.c -o client

nameserver: nameserver.c common.h
	$(CC) $(CFLAGS) -pthread nameserver.c -o nameserver

storageserver: storageserver.c common.h
	$(CC) $(CFLAGS) -pthread storageserver.c -o storageserver

clean:
	rm -f client nameserver storageserver *.log
