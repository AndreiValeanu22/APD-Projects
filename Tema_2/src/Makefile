CC = mpicc
TARGET = tema2
CFLAGS = -Wall -Wextra -O2
SRC = tema2.c

all: build

build: $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -f $(TARGET) *.o

.PHONY: all clean run
