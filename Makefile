CC := gcc
CFLAGS := -Wall -Wextra
LDFLAGS := -ltdjson -lpthread -lm -ldl

SRC := src/main.c
OBJ := build/main.o
TARGET := bot

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(OBJ): $(SRC)
	mkdir -p build
	$(CC) $(CFLAGS) -c -o $@ $<

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf build $(TARGET) logs
