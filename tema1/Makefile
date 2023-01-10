CC = gcc
CFLAGS = -Wall -Werror -g

build:
	$(CC) ${CFLAGS} -o tema1 tema1.cpp -lstdc++ -ldl -lm -lpthread

run: build
	./tema1
	
.PHONY: clean

clean:
	rm -f tema1