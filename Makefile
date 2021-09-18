CFLAGS=-Iinclude -Llib
LIB=-lrequest
INC = ./include
OBJ = ./object
SRC = ./src

server: $(SRC)/server.c $(INC)/sql_queries.c
	gcc -o server1 $(CFLAGS) $(SRC)/server.c $(INC)/sql_queries.c $(LIB)

clean: 
	rm -rf *.o server1
