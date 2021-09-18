#include "sql_queries.h"

const char *file_path = "./database/";
const char *table_file_path = "./database/tables.txt";
char *schema_file_path = "./database/schema.txt";
char *temp_table_file_path = "./database/temp_tables.txt";
char *temp_schema_file_path = "./database/temp_schema.txt";
char * tempPath = "./database/tempfile.txt";
char *tempInsertPath = "tempInsertfile.txt";
char * UpdateTempPath = "./database/Updatetempfile.txt";


char * tables(){

	FILE *file = fopen(table_file_path, "r");
	int count = 0;
	char * data = (char *)malloc(sizeof(char) * 1000);

	memset(data, '\0', 1000);

	printf("\n////////////////////////////////////////////////////////\n");
	printf("                           Tables                         \n");
	if(file != NULL){
	
		char ch;
		ch = fgetc(file);

		while(!feof(file)){
			sprintf(data, "%s%c", data, ch);
			printf("%c", ch);
			ch = fgetc(file);
			count++;
		}

		if(count == 0){
			sprintf(data, "No table found");
			printf("No table found");
		}
		
		sprintf(data, "%s\n", data);
		printf("\n");
	}
	else{
		sprintf(data, "File not Found!!!...\n");
		printf("File not Found!!... Create one");
	}
	printf("\n////////////////////////////////////////////////////////\n");

	return data;
}

char * schema(char * table_name){

	FILE *file =  fopen(schema_file_path, "r");
	int found = 0;
	char * buffer = (char *)malloc(sizeof(char) * 10000);
	memset(buffer, '\0', 10000);

	printf("\n////////////////////////////////////////////////////////\n");
	printf("                           Schema %s                      \n", table_name);

	if(file != NULL){
	
		char ch;
		char data[1000];

		while(!feof(file)){
			fscanf(file, "%s", data);
			if(strcmp(data, table_name) == 0){

				printf("%s", data);
				sprintf(buffer, "%s%s", buffer, data);	
				while((ch = fgetc(file)) != '}' && !feof(file)){
					sprintf(buffer, "%s%c", buffer, ch);
					printf("%c", ch);
				}
				found = 1;
				sprintf(buffer, "%s}\n", buffer);
				break;

			}
			else 
				while((ch = fgetc(file)) != '}' && !feof(file));
		}

		if(!found){
			sprintf(buffer, "Schema not found!!!\n");
			printf("Schema not found!!!");
		}
		else printf("%c", ch);

		printf("\n");
	}
	else{
		sprintf(buffer, "File not found!!!\n");
		printf("File not Found!!... Create one");
	}

	printf("\n////////////////////////////////////////////////////////\n");

	return buffer;
}

int create_query(request_t * request){

	FILE * file = fopen(table_file_path, "r");

    	if(file != NULL){

    		char buffer[100];
    		int found = 0;

    		while(!feof(file)){
    			memset(buffer, '\0', 100);
    			fscanf(file, "%s[^/n]", buffer);
    			if(strcmp(buffer, request->table_name) == 0){
    				found = 1;
    				break;
    			}
    		}

    		if(!found){

    			fclose(file);
    			file = fopen(table_file_path, "a");
    			fprintf(file, "%s\n", request->table_name);
                	fclose(file);
                	file = fopen(schema_file_path, "a");
                	fprintf(file, "%s\n", request->table_name);
		        fprintf(file, "{\n");
		        column_t * temp = request->columns;

		        while(temp != NULL){
		             fprintf(file, "%s %s ", temp->name, (temp->data_type == DT_INT) ? "INTEGER" : "VARCHAR");
		             if(temp->data_type == DT_VARCHAR){
		                fprintf(file, "( %d )", temp->char_size);
		             }
		             if(temp->is_primary_key == DT_VARCHAR){
		                fprintf(file, "PK");
		             }
		             temp = temp->next;
		             fprintf(file, "\n");
		        }

               		fprintf(file, "}\n");
    			fclose(file);
    			memset(buffer, '\0', 100);
    			sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
    			file = fopen(buffer, "a");
		        temp = request->columns;

		        while(temp != NULL){
		         fprintf(file, "%s", temp->name);
		         temp = temp->next;
		         if(temp != NULL)
		            fprintf(file, " ");
		        }
		        fclose(file);
			return 1;
    		}
    		else {
    			fclose(file);
			return 0;    			
    		}
    	}
}

int drop_query(request_t * request){

	char buffer[100];
        memset(buffer, '\0', 100);
        FILE * file = fopen(table_file_path, "r");
        int found = 0;
        
        while(!feof(file)){

            fscanf(file, "%s", buffer);
            if(strcmp(buffer, request->table_name) == 0){
                found = 1;
                memset(buffer, '\0', 100);
                sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
                remove(buffer);
                break;
            }
        }

        if(found){

            fclose(file);
            FILE * temp_file = fopen(temp_table_file_path, "w");
            file = fopen(table_file_path, "r");

            while(!feof(file)){

                memset(buffer, '\0', 100);
                fscanf(file, "%s", buffer);
                if(strlen(buffer) > 0 && strcmp(buffer, request->table_name) != 0){
                    fprintf(temp_file, "%s\n", buffer);
                }
            }

            fclose(file);
            remove(table_file_path);
            rename(temp_table_file_path, table_file_path);
            fclose(temp_file);
            file = fopen(schema_file_path, "r");
            temp_file = fopen(temp_schema_file_path, "w");
            char data[1000];

            while(!feof(file)){

                memset(data, '\0', 1000);
                fscanf(file, "%s", data);
                if(strlen(data) > 0 && strcmp(data, request->table_name) != 0){
		    fseek(file, -(int)(strlen(data)), SEEK_CUR);
                    char ch;
                    int count = 0;
                    while(!feof(file) &&(ch = fgetc(file)) != '}'){
                        printf("%c", ch);
                        fputc(ch, temp_file);
                    }
                    fputc(ch, temp_file);
                    fprintf(temp_file, "\n");       
                }
		else {
			char ch;
			while(!feof(file) &&(ch = fgetc(file)) != '}');
		}

            }
            fclose(file);
            remove(schema_file_path);
            rename(temp_schema_file_path, schema_file_path);
            fclose(temp_file);
	    return 1; 
        }
        else {
            fclose(file);
	    return 0;
        }
}

int delete_query(request_t * request){

	char buffer[100];
        memset(buffer, '\0', 100);
        sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
        FILE * file = fopen(buffer, "r");
        FILE * temp_file = fopen(tempPath, "w");

        if(file != NULL){
            char data[1000];
            memset(data, '\0', 1000);
	    int found = 0;
            char ch = '\0';

            while(!feof(file) && (ch = fgetc(file)) != '\n'){
                fputc(ch, temp_file);
            }

	    char prev_data[1000];
	    memset(prev_data, '\0', 1000);

            while(!feof(file)){
		sprintf(prev_data, "%s", data);
                fscanf(file, "%s", data);
		
		if(strcmp(data, prev_data) == 0)
			break;


                if(atoi(data) != request->where->int_val){
                	fprintf(temp_file, "\n");
                    	fprintf(temp_file, "%s", data);
                    
			while((ch = fgetc(file)) != '\n' && !feof(file)){
				printf("%c", ch);
                        	fputc(ch, temp_file);
                    	}
			memset(data, '\0', 100);
                }
                else {
                     found = 1;
                     while((ch = fgetc(file)) != '\n' && !feof(file));
                }
            }

	    fclose(file);
            remove(buffer);
            rename(tempPath, buffer);
            fclose(temp_file);

            if(found){
		return 1;
            }
            else {
		return 0;
            }    
        }
        else {
	    return -1;
        }
}

int insert_query(request_t * request){

	char buffer[100];
        memset(buffer, '\0', 100);
        sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
        FILE * file = fopen(buffer, "r");

	if(file == NULL) return -1;

        FILE * tempfile = fopen(tempInsertPath, "w");
        char ch = '\0';
        char data[1000];

        while((ch = fgetc(file)) != '\n' && !feof(file)){
            fputc(ch, tempfile);
        }

        int found = 0;

        while(!feof(file)){
            fprintf(tempfile, "\n"); 
            memset(data, '\0', 1000);
            fscanf(file, "%s", data);

            if(strlen(data) == 0)
            	break;

            if(atoi(data) == request->columns->int_val){
                found = 1;
                break;

            }
          
	    fseek(file, -((int)strlen(data)), SEEK_CUR);
            
	    while((ch = fgetc(file)) != '\n' && !feof(file)){
                fputc(ch, tempfile);
            }      
        }

        if(found){
            fclose(file);
            fclose(tempfile);
            remove(tempInsertPath);
	    return 0;            
        }
        else {
            fclose(file);
            remove(buffer);
	    fprintf(tempfile, "\n"); 
            column_t * temp = request->columns;

            while(temp != NULL){
		    if(temp->data_type == DT_INT){
		    	fprintf(tempfile, "%d", temp->int_val);
		    }
		    else {
		    	fprintf(tempfile, "%s", temp->char_val);
		    }
		    temp = temp->next;
		    if(temp != NULL)
		    	fprintf(tempfile, " ");
	    }      
            rename(tempInsertPath, buffer);
            fclose(tempfile);
	    return 1;
        }
}

char * select_query(request_t * request){

	char * output = (char *)malloc(sizeof(char) * 1000);

	memset(output, '\0', 1000);

	if(request->columns == NULL){
    		char buffer[100];
    		memset(buffer, '\0', 100);
    		sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
    		FILE * file = fopen(buffer, "r");

    		if(file != NULL){
    			char ch = '\0';
			ch = fgetc(file);
    			while(!feof(file)){
				sprintf(output, "%s%c", output, ch);
    				printf("%c", ch);
				ch = fgetc(file);
    			}
    			fclose(file);
    		}
    		else{
    			printf("Table %s not found\n", request->table_name);
			sprintf(output, "Table %s not found\n", request->table_name);
		}
    	}
    	else {

		if(request->columns != NULL){
    			char buffer[100];
    			memset(buffer, '\0', 100);
    			sprintf(buffer, "./%s/%s.txt", file_path, request->table_name);
    			FILE * file = fopen(buffer, "r");

    			if(file != NULL){
    				int count = 0;
    				column_t *temp = request->columns;

			    	while(temp != NULL){
			    		count++;
			    		temp = temp->next;
			   	 }
			    	temp = request->columns;
			    	char **argss = (char **)malloc(count * sizeof(char));
			    	int index = 0;

			    	while(temp != NULL){
			    		argss[index++] = temp->name;
			    		temp = temp->next;
			    	}
			    	int *indexes = (int *)malloc(count * sizeof(int));
			    	int int_index = 0;
    				char data[1000];
    				char ch = '\0';
				index = 0;
    				memset(data, '\0', 1000);
				printf("%d\n", count);

    				while(!feof(file) && (ch = fgetc(file)) != '\n'){

					if(ch == ' '){
						for(int i = 0; i < count; i++){
							if(strcmp(data, argss[i]) == 0){
								indexes[int_index++] = i + 1;
								printf("%s %d ", data, i);
								sprintf(output, "%s%s ", output, data);
							}
						}
						memset(data, '\0', 1000);
						index = 0;
					}
					else
						data[index++] = ch;
    				}

				
				for(int i = 0; i < count; i++){	
					if(strcmp(data, argss[i]) == 0){	
						indexes[int_index++] = i + 1;
						printf("%s ", data);
						sprintf(output, "%s%s ", output, data);	
					}
				}
				sprintf(output, "%s\n", output);
				printf("\n");
				index = int_index = 0;
				memset(data, '\0', 1000);
				int val_index = 0;
				ch = fgetc(file);

				while(!feof(file)){

					if(ch == ' '){
						if(val_index == indexes[int_index]){
							printf("%s ", data);
							sprintf(output, "%s%s", output, data);
							int_index++;
						}
						val_index++;

						memset(data, '\0', 1000);
						index = 0;
					}
					else if(ch == '\n'){
						val_index = 0;
						printf("%s", data);
						printf("\n");
					}
					else{
						data[index++] = ch;
					}
					ch = fgetc(file);
    				}

				if(val_index == indexes[int_index]){
					sprintf(output, "%s%s", output, data);
					printf("%s ", data);
				}
				printf("\n");
				sprintf(output, "%s\n", output);
    				fclose(file);
    			}
    			else{
				sprintf(output, "Table %s not found\n", request->table_name);
    				printf("File not found\n");  
			} 		

    		}
   	 }

	return output;
}

int update_query(request_t * request){

	char buffer[100];
        memset(buffer, '\0', 100);
        sprintf(buffer, "./%s/%s.txt", file_path,request->table_name);
        FILE * file = fopen(buffer, "r");
        FILE * temp_file = fopen(UpdateTempPath, "w");

        if(file != NULL){
            char data[1000];
	    char prev_data[1000];
            memset(data, '\0', 1000);
	    memset(prev_data, '\0', 1000);
            int found = 0;
            char ch = '\0';

            while(!feof(file) && (ch = fgetc(file)) != '\n'){
                fputc(ch, temp_file);
            }

            while(!feof(file)){
                fscanf(file, "%s", data);
		if(prev_data != NULL && strcmp(prev_data, data) == 0)
			break;
                if(atoi(data) != request->where->int_val){
                	fprintf(temp_file, "\n");
                        fprintf(temp_file, "%s", data);
                    
			while((ch = fgetc(file)) != '\n' && !feof(file)){
                        	fputc(ch, temp_file);
                    	}
                }
                else {
		     fprintf(temp_file, "\n");
                     found = 1;
                     while((ch = fgetc(file)) != '\n' && !feof(file));
		     column_t * cols = request->columns;
		     fprintf(temp_file, "%s ", data);

		     while(cols != NULL){
			if(cols->data_type == DT_INT)
				fprintf(temp_file, "%d", cols->int_val);
			else
			     fprintf(temp_file, "%s", cols->char_val);
			cols = cols->next;
			if(cols != NULL) fprintf(temp_file, " ");
		     }
                }
		memset(prev_data, '\0', 1000);
		sprintf(prev_data, "%s", data);
            }
	    fclose(file);
            remove(buffer);
            rename(UpdateTempPath, buffer);
            fclose(temp_file);

            if(found){
		return 1;
            }
            else {
		return 0;
            }
        }
        else {
            return -1;
        }
}
