#ifndef SQL_QUERIES_H_ 
#define SQL_QUERIES_H_
#include "request.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>



char * tables();
char * schema(char *);
int create_query(request_t *);
int drop_query(request_t *);
int delete_query(request_t *);
int insert_query(request_t *);
char * select_query(request_t *);
int update_query(request_t *);

#endif 
