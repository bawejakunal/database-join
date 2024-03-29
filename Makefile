CC = gcc
CFLAGS = -O3 -Wall
LIBS = -lpthread

SRC=$(wildcard *.c)

all:	q4112 q4112_hj q4112_nlj_1 q4112_nlj q4112_hj_1

q4112:	q4112.o q4112_gen.o q4112_main.o
	$(CC) $(CFLAGS) -o q4112 q4112.o q4112_gen.o q4112_main.o $(LIBS)

q4112_hj:	q4112_hj.o q4112_gen.o q4112_main.o
	$(CC) $(CFLAGS) -o q4112_hj q4112_hj.o q4112_gen.o q4112_main.o $(LIBS)

q4112_nlj_1:	q4112_nlj_1.o q4112_gen.o q4112_main.o
	$(CC) $(CFLAGS) -o q4112_nlj_1 q4112_nlj_1.o q4112_gen.o q4112_main.o $(LIBS)

q4112_nlj:	q4112_nlj.o q4112_gen.o q4112_main.o
	$(CC) $(CFLAGS) -o q4112_nlj q4112_nlj.o q4112_gen.o q4112_main.o $(LIBS)

q4112_hj_1:	q4112_hj_1.o q4112_gen.o q4112_main.o
	$(CC) $(CFLAGS) -o q4112_hj_1 q4112_hj_1.o q4112_gen.o q4112_main.o $(LIBS)

## Object files
q4112.o:	q4112.c
	$(CC) $(CFLAGS) -c q4112.c

q4112_hj.o:	q4112_hj.c
	$(CC) $(CFLAGS) -c q4112_hj.c

q4112_nlj_1.o:	q4112_nlj_1.c
	$(CC) $(CFLAGS) -c q4112_nlj_1.c

q4112_nlj.o:	q4112_nlj.c
	$(CC) $(CFLAGS) -c q4112_nlj.c

q4112_hj_1.o:	q4112_hj_1.c
	$(CC) $(CFLAGS) -c q4112_hj_1.c

q4112_main.o:	q4112_main.c q4112.h
	$(CC) $(CFLAGS) -c q4112_main.c

clean:
	$(RM) $(SRC:.c=.o) $(SRC:.c=)