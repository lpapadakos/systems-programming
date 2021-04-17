CFLAGS = -Wall -pedantic -O3

COMMON_HDR = $(wildcard include/*.h)
COMMON_SRC = $(wildcard src/*.c)

MASTER_HDR = $(wildcard include/master/*.h)
MASTER_SRC = $(wildcard src/master/*.c)

SERVER_HDR = $(wildcard include/server/*.h)
SERVER_SRC = $(wildcard src/server/*.c)

CLIENT_HDR = $(wildcard include/client/*.h)
CLIENT_SRC = $(wildcard src/client/*.c)

all: master server client

master: $(COMMON_HDR) $(COMMON_SRC) $(MASTER_HDR) $(MASTER_SRC)
	$(CC) -I ./include $(CFLAGS) $^ -o master

server: $(COMMON_HDR) $(COMMON_SRC) $(SERVER_HDR) $(SERVER_SRC)
	$(CC) -I ./include $(CFLAGS) $^ -o whoServer -pthread

client: $(COMMON_HDR) $(COMMON_SRC) $(CLIENT_HDR) $(CLIENT_SRC)
	$(CC) -I ./include $(CFLAGS) $^ -o whoClient -pthread

clean:
	$(RM) master whoServer whoClient
