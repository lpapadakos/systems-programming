CFLAGS = -Wall -pedantic

SRC = $(wildcard src/*.c)
HDR = $(wildcard include/*.h)
TARGET = diseaseAggregator

$(TARGET): $(SRC) $(HDR)
	$(CC) $(CFLAGS) -I ./include/ $(SRC) -o $(TARGET) -g3

clean:
	$(RM) $(TARGET)
