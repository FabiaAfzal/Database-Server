#include "../include/sql_queries.h"

#include <sys/types.h>

#include <sys/socket.h>

#include <arpa/inet.h>

#include <netinet/in.h>

#include <sys/ipc.h>

#include <sys/shm.h>

#include<pthread.h>

#include <sys/types.h>

#include <sys/wait.h>

#define SA struct sockaddr
#define MAXLINE 1024
#define PORT 8081
char * Logging_path = "./database/Logging.txt";

void handle_sigint(int sig) 
{ 
 char path[1035];
 //   kill(getpid(),SIGKILL);
 
	FILE *fp;
 	

char abc[1000];
 

  fp = popen("pgrep -f server", "r");
  if (fp == NULL) {
    printf("Failed to run command\n" );
    exit(1);
  }

int  i=0;
  while (fgets(path, sizeof(path), fp) != NULL) {

		strcpy(abc,"kill -1 ");
  		  
			strcat (abc,path);
		if(i!=1){printf("\n[%s]", path);	system(abc);
		}

i++;
strcpy(abc,"");
  }

 
  pclose(fp);

} 


typedef struct keyNode keyNode;
struct keyNode {
    int key;
    char * table_name;
    pthread_mutex_t * mutex;
    keyNode * next;
};

typedef struct keyList keyList;

struct keyList {

    keyNode * head;

};

keyList * list;

int key = 1234;

int hasNode(keyList * node, char * table_name) {

    if (node == NULL)
        return -1;

    keyNode * temp = node -> head;

    while (temp != NULL) {

        if (strcmp(temp -> table_name, table_name) == 0)
            return 1;
        temp = temp -> next;
    }

    return 0;

}

void generateKey(char * table_name) {

    pthread_mutex_t * mutex;

    int error = 0;

    int shmid = 0;

    while (!error) {

        error = 1;

        if ((shmid = shmget(key, sizeof(pthread_mutex_t), IPC_CREAT | 0666)) < 0) {
            error = 0;
        }

        if ((mutex = shmat(shmid, NULL, 0)) == (pthread_mutex_t * ) - 1) {

            error = 0;
        }

        key++;

    }

    if (!hasNode(list, table_name))
        return;

    keyNode * temp = list -> head;

    if (temp == NULL) {

        temp = malloc(sizeof(keyNode));
        temp -> key = key - 1;
        temp -> table_name = table_name;
        temp -> mutex = mutex;
        temp -> next = NULL;

    } else {

        while (temp -> next != NULL)
            temp = temp -> next;

        temp -> next = malloc(sizeof(keyNode));
        temp -> next -> key = key - 1;
        temp -> next -> mutex = mutex;
        temp -> table_name = table_name;
        temp = temp -> next;
        temp -> next = NULL;

    }

    printf("Key generated for Table %s is %d\n", table_name, key - 1);

}
void handler1(int sig)
{
  pid_t pid;

  pid = wait(NULL);

  printf("Pid %d exited.\n", pid);
}

void get_Table_Keys() {

    FILE * file = fopen("./database/tables.txt", "r");

    if (file != NULL) {

        char data[1000];

        while (!feof(file)) {
            memset(data, '\0', 1000);
            fscanf(file, "%s", data);

            if (strlen(data) > 0)
                generateKey(data);

        }

    } else {
        printf("File not Found\n");
    }

}

void AcquireLock(char * table_name,
    const char Operation) {

    keyNode * temp = list -> head;

    if (temp == NULL)
        return;

    else {

        while (temp != NULL) {

            if (strcmp(temp -> table_name, table_name) == 0) {

                pthread_mutex_lock(temp -> mutex);
                break;
            }
            temp = temp -> next;
        }

    }

}

void ReleaseLock(char * table_name,
    const char Operation) {

    keyNode * temp = list -> head;

    if (temp == NULL)
        return;

    else {

        while (temp != NULL) {

            if (strcmp(temp -> table_name, table_name) == 0) {

                pthread_mutex_unlock(temp -> mutex);
                break;
            }
            temp = temp -> next;
        }

    }
}

void Sql_Server_Operations(request_t * request, int connfd, FILE * fptr) {

    if (request -> request_type == RT_TABLES) {

        //AcquireLock(request->table_name, RT_TABLES);

        char * data = tables();

        send(connfd, data, strlen(data), 0);

        fprintf(fptr, "Command: List all Tables\nResult\n%s\n", data);

        free(data);

        //ReleaseLock(request->table_name, RT_TABLES);

    } else if (request -> request_type == RT_SCHEMA) {

        AcquireLock(request -> table_name, RT_SCHEMA);

        char * data = schema(request -> table_name);

        send(connfd, data, strlen(data), 0);

        fprintf(fptr, "Command: List schema for %s table\nResult\n%s\n", request -> table_name, data);

        free(data);

        ReleaseLock(request -> table_name, RT_SCHEMA);

    } else if (request -> request_type == RT_CREATE) {

        AcquireLock(request -> table_name, RT_CREATE);

        int status = create_query(request);
        char data[1000];

        memset(data, '\0', 1000);

        if (status) {
            generateKey(request -> table_name);
            sprintf(data, "Table %s.txt Successfully Created\n", request -> table_name);
            fprintf(fptr, "Command: Create Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Table %s.txt Successfully Created\n", request -> table_name);
        } else {
            sprintf(data, "Cant Create Table %s!!! Already Exists\n", request -> table_name);
            fprintf(fptr, "Command: Create Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Cant Create Table %s!!! Already Exists\n", request -> table_name);
        }

        send(connfd, data, strlen(data), 0);

        ReleaseLock(request -> table_name, RT_CREATE);

    } else if (request -> request_type == RT_DROP) {

        AcquireLock(request -> table_name, RT_DROP);

        int status = drop_query(request);

        char data[1000];

        memset(data, '\0', 1000);

        if (status) {
            sprintf(data, "Successfully Deleted Table %s\n", request -> table_name);
            fprintf(fptr, "Command: Drop Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Successfully Deleted Table %s\n", request -> table_name);
        } else {
            sprintf(data, "Table %s not found!!! Can't Delete\n", request -> table_name);
            fprintf(fptr, "Command: Drop Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Table %s not found!!! Can't Delete\n", request -> table_name);
        }

        send(connfd, data, strlen(data), 0);
        ReleaseLock(request -> table_name, RT_DROP);

    } else if (request -> request_type == RT_DELETE) {

        AcquireLock(request -> table_name, RT_DELETE);

        int status = delete_query(request);
        char data[1000];

        memset(data, '\0', 1000);

        if (status < 0) {
            sprintf(data, "Table %s not Found!!! Can't Delete Row\n", request -> table_name);
            fprintf(fptr, "Command: Delete row from Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Table %s not Found!!! Can't Delete Row\n", request -> table_name);
        } else if (status == 0) {
            sprintf(data, "Row not found\n");
            fprintf(fptr, "Command: Delete row from Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Row not found\n");
        } else {
            sprintf(data, "Row Successfully Deleted from table %s\n", request -> table_name);
            fprintf(fptr, "Command: Delete row from Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Row Successfully Deleted from table %s\n", request -> table_name);
        }

        send(connfd, data, strlen(data), 0);

        ReleaseLock(request -> table_name, RT_DELETE);

    } else if (request -> request_type == RT_INSERT) {

        AcquireLock(request -> table_name, RT_INSERT);

        char data[1000];

        memset(data, '\0', 1000);

        int status = insert_query(request);

        if (status < 0) {
            sprintf(data, "Table %s not Found!!! Can't Insert Row\n", request -> table_name);
            fprintf(fptr, "Command: Insert row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Table %s not Found!!! Can't Insert Row\n", request -> table_name);
        } else if (status == 0) {
            sprintf(data, "Can't insert Row!!! Already Exists in Table %s!\n", request -> table_name);
            fprintf(fptr, "Command: Insert row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Can't insert Row!!! Already Exists in Table %s!\n", request -> table_name);
        } else {
            sprintf(data, "Row Successfully Inserted in Table %s\n", request -> table_name);
            fprintf(fptr, "Command: Insert row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Row Successfully Inserted in Table %s\n", request -> table_name);
        }

        send(connfd, data, strlen(data), 0);

        ReleaseLock(request -> table_name, RT_INSERT);

    } else if (request -> request_type == RT_SELECT) {

        AcquireLock(request -> table_name, RT_SELECT);

        char * output = select_query(request);

        send(connfd, output, strlen(output), 0);

        fprintf(fptr, "Command: to display data of Table %s\nResult\n%s\n", request -> table_name, output);

        free(output);

        ReleaseLock(request -> table_name, RT_SELECT);

    } else if (request -> request_type == RT_UPDATE) {

        AcquireLock(request -> table_name, RT_UPDATE);

        char data[1000];

        memset(data, '\0', 1000);

        int status = update_query(request);

        if (status < 0) {
            sprintf(data, "Table %s not Found!!! Can't Update Row\n", request -> table_name);
            fprintf(fptr, "Command: Update row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Table %s not Found!!! Can't Update Row\n", request -> table_name);
        } else if (status == 0) {
            sprintf(data, "Row not found in Table %s\n", request -> table_name);
            fprintf(fptr, "Command: Update row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Row not found in Table %s\n", request -> table_name);
        } else {
            sprintf(data, "Row Successfully Updated in Table %s\n", request -> table_name);
            fprintf(fptr, "Command: Update row in Table %s\nResult\n%s\n", request -> table_name, data);
            printf("Row Successfully Updated in Table %s\n", request -> table_name);
        }

        send(connfd, data, strlen(data), 0);

        ReleaseLock(request -> table_name, RT_UPDATE);

    }

}

int main(int argc, char * argv[]) {

 signal(SIGINT, handle_sigint);
if(argc == 1)
{

printf("Invalid Number of Argument\n");
exit(0);

return 0;
}

int portc = 0;

int l=1;
		for(;l<argc ;l++)
		{
			if(strcmp(argv[l],"-p")==0)
			{
				if(strcmp(argv[l+1],"8081")==0)
				{
						portc = 1;
				}

				else
				{
					printf("\n\nInvalid Port\n\n");	
			printf("\nUsage <param>  param [-h | -p <port>]\n\n\n");

				return 0;
				}

			}

printf("[%s]",argv[l]);
			 if(strcmp(argv[l],"-h")==0)
			{

				printf("\nThe server must support at least the 	following command line options:\n-h Print help text\n-p port Listen to port number port.\n-d Run as a daemon instead of as a normal program.\n-llogfile Log to logfile. \n-s [fork | thread | prefork | mux] \n\n\n\n");
			
		
	    
			}
		
	

		}
if(portc==0){ printf("\nUsage <param>  param [-h | -p <port>]\n\n\n"); return 0;}



    int status = 0;

    int shmid = 0;
    int sharedkey = 1111;
    int sockfd = 0;
    int connfd = 0;
    char buffer[MAXLINE];
    pid_t pid;
    struct sockaddr_in servaddr, cliaddr;
    socklen_t addr_size;
    list = malloc(sizeof(keyList));

    list -> head = NULL;

    get_Table_Keys();

    if ((shmid = shmget(sharedkey, sizeof(pthread_mutex_t), IPC_CREAT | 0666)) < 0) {
        exit(-1);
    }

    if ((list = shmat(shmid, NULL, 0)) == (keyList * ) - 1) {
        exit(-1);
    }

    // Creating socket file descriptor 
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset( & servaddr, 0, sizeof(servaddr));
    //memset(&cliaddr, 0, sizeof(cliaddr)); 

    // Filling server information 
    servaddr.sin_family = AF_INET; // IPv4 

    servaddr.sin_port = htons(PORT);
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    // Bind the socket with the server address 
    if (bind(sockfd, (struct sockaddr * ) & servaddr, sizeof(servaddr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(sockfd, 100) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

  
    while (1) {
        status = 0;
        connfd = accept(sockfd, (struct sockaddr * ) & cliaddr, & addr_size);
        if (connfd < 0) {
            exit(1);
        }
        printf("Connection accepted from %s:%d\n", inet_ntoa(cliaddr.sin_addr), ntohs(cliaddr.sin_port));
signal(SIGCHLD, handler1);
        if ((pid = fork()) == 0) {
            close(sockfd);

            while (1) {

                memset(buffer, '\0', MAXLINE);
                int n = recv(connfd, buffer, MAXLINE, 0);
                if (strcmp(buffer, ".quit") == 0) {
printf("Disconnected from %s:%d\n", inet_ntoa(cliaddr.sin_addr), ntohs(cliaddr.sin_port));
sleep(1);



                } else {

                    //int n = recv(connfd, buffer, MAXLINE, 0);

                    buffer[n] = '\0';

                    request_t * request;

                    char * error;

                    request = parse_request(buffer, & error);

                    FILE * fptr = fopen(Logging_path, "a");

                    if (request == NULL) {


                    } 
                     	
                	else if (request -> request_type == RT_QUIT) {
                            printf("Client with ip %s and port %d has decided to terminate\n\n", inet_ntoa(cliaddr.sin_addr), ntohs(cliaddr.sin_port));
                   	
			     send(connfd, "Close by server", strlen("Close by server"), 0);
			sleep(1);	wait(NULL); break;
                        }
   				    

            else
{ 
                     print_request(request);
    Sql_Server_Operations(request, connfd, fptr);

                  			  fclose(fptr);
}
                }

            }
 	
     
     sleep(1);
     wait(&status);
     //  exit(0);
        }

    
    
    
    }

    close(connfd);

    return 0;

}
