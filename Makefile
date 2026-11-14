CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2
LDFLAGS = -pthread -lrt

TARGET = campuscompute
SOURCE = campus_compute.c

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) $(SOURCE) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean


