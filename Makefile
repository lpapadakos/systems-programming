CFLAGS = -Wall -Wno-missing-braces -pedantic

SRC = $(wildcard src/*.c)
HDR = $(wildcard include/*.h)
TARGET = diseaseMonitor

$(TARGET): $(SRC) $(HDR)
	$(CC) $(CFLAGS) -I ./include/ $(SRC) -o $(TARGET) -g

clean:
	$(RM) $(TARGET)
