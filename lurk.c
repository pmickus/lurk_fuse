/* 
 gcc -Wall `pkg-config fuse --cflags --libs` `mysql_config --cflags` lurk.c -o lurk_fuse `mysql_config --libs`
*/

#define FUSE_USE_VERSION 27
#define _XOPEN_SOURCE 500  

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <fuse.h>
#include <regex.h>
#include <mysql.h>

#define MAXPATH 1024
#define MAXQUERY 1024

typedef enum {
    ROOTDIR,
    SERIESROOTDIR,
    BROWSEDIR,  
    SERIESDIR,
    NRDIR,
    LFILE
} lurk_path_t;

typedef struct stack {
    char path[MAXPATH];
    struct stack *next;
} stack_t;


void* mymalloc(unsigned int size);
stack_t* push(stack_t *stack, char *path);
stack_t* pop(stack_t *stack);
MYSQL* lurk_mysql_get_connect();
stack_t* lurk_mysql_get_files(const char *series);
stack_t* lurk_mysql_get_nr_files();
stack_t* lurk_mysql_get_series(const char *series);
char* lurk_mysql_get_path(const char *filename);
char* get_last_slash(char *path);
int lurk_path_parse(const char *path);
stack_t* lurk_get_dirs(const lurk_path_t lurkv, const char *path);
static int lurk_getattr(const char *path, struct stat *stbuf);
static int lurk_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
static int lurk_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
static int lurk_open(const char *path, struct fuse_file_info *fi);

stack_t* push(stack_t *stack, char *path)
{

    stack_t *new;
    
    new = mymalloc(sizeof(stack_t));

    if (strlen(path) >= MAXPATH) 
       return NULL;

    strcpy(new->path, path);
    new->next = stack;

    return new;
}

stack_t* pop(stack_t *stack)
{

    stack_t *temp;
   
    temp = stack;
    stack = stack->next;
    
    free(temp);

    return stack;
}

int free_stack(stack_t *stack)
{
    while (stack != NULL)
        stack = pop(stack);

    return 0;
}


void* mymalloc(unsigned int size) 
{

    char *ptr;

    ptr = malloc(size);

    if (!ptr) {
       puts("No memory!!!");
       exit(EXIT_FAILURE);
    }

    if (ptr)
       memset(ptr, '\0', size);

    return ptr;

}   

MYSQL* lurk_mysql_get_connect() 
{

     MYSQL *conn;
     char *server = "localhost";
     char *user = "user";
     char *password = "password";
     char *database = "lurk";
   
     conn = mysql_init(NULL);
   
     /* Connect to database */
     if (!mysql_real_connect(conn, server, user, password, database, 0, NULL, 0)) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        return NULL;
     }

     return conn;
}

stack_t* lurk_mysql_get_files(const char *series)
{

     MYSQL *conn;
     MYSQL_RES *res;
     MYSQL_ROW row;

     int ret=0;
     stack_t* stack = NULL;
     char *qseries;
     char query[MAXQUERY];

     if (series == NULL)
        return NULL;

     if ((conn = lurk_mysql_get_connect()) == NULL)
        return NULL;

     qseries = mymalloc((strlen(series) * 2) + 1);
     
     mysql_real_escape_string(conn, qseries, series, strlen(series));
   
     if ((strlen(qseries) + 186) >= MAXQUERY) {
        fprintf(stderr, "QUERY TOO LONG\n");
        free(qseries);
        mysql_close(conn);
        return NULL;
     }
   
     memset(query, '\0', MAXQUERY);   
     sprintf(query, "select path from filepaths, files, files_series, series where files.filename=filepaths.filename and files.id=files_series.file_id and series.id=files_series.series_id and series.name='%s'", qseries);
     

     if ((ret = mysql_query(conn, query)) != 0) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        free(qseries);
        mysql_close(conn);
        return NULL;
     }

     free(qseries);
     res = mysql_use_result(conn);
   
     while ((row = mysql_fetch_row(res)) != NULL) {
         if ((stack = push(stack, row[0])) == NULL) {
            mysql_free_result(res);
            mysql_close(conn);        
            free_stack(stack);
            return NULL;
         }
     }

     mysql_free_result(res);
     mysql_close(conn); 

     return stack;
}

stack_t* lurk_mysql_get_nr_files()
{

     MYSQL *conn;
     MYSQL_RES *res;
     MYSQL_ROW row;

     int ret=0;
     stack_t* stack = NULL;
     char query[MAXQUERY];

     if ((conn = lurk_mysql_get_connect()) == NULL)
        return NULL;
   
     strcpy(query, "select path from filepaths, new_releases where new_releases.filename=filepaths.filename");

     if ((ret = mysql_query(conn, query)) != 0) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        mysql_close(conn);
        return NULL;
     }

     res = mysql_use_result(conn);
   
     while ((row = mysql_fetch_row(res)) != NULL) {
         if ((stack = push(stack, row[0])) == NULL) {
            mysql_free_result(res);
            mysql_close(conn);        
            free_stack(stack);
            return NULL;
         }
     }

     mysql_free_result(res);
     mysql_close(conn); 

     return stack;
}

stack_t* lurk_mysql_get_series(const char *series)
{

     MYSQL *conn;
     MYSQL_RES *res;
     MYSQL_ROW row;

     int ret=0;
     stack_t* stack = NULL;
     char *qseries;
     char query[MAXQUERY];

     if (series == NULL)
        return NULL;

     if ((conn = lurk_mysql_get_connect()) == NULL)
        return NULL;

     qseries = mymalloc((strlen(series) * 2) + 1);
     
     mysql_real_escape_string(conn, qseries, series, strlen(series));
   
     if ((strlen(qseries) + 45) >= MAXQUERY) {
        fprintf(stderr, "QUERY TOO LONG\n");
        free(qseries);
        mysql_close(conn);
        return NULL;
     }
   
     if (strcmp(series, "0-9") == 0)  {
        strcpy(query, "select name from series where name regexp '^[0-9\\+]'");      
     }
     else {  
        memset(query, '\0', MAXQUERY);  
        sprintf(query, "select name from series where name regexp '^%s'", qseries);
     }

     if ((ret = mysql_query(conn, query)) != 0) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        free(qseries);
        mysql_close(conn);
        return NULL;
     }

     free(qseries);
     res = mysql_use_result(conn);
   
     while ((row = mysql_fetch_row(res)) != NULL) {
         if ((stack = push(stack, row[0])) == NULL) {
            mysql_free_result(res);
            mysql_close(conn);        
            free_stack(stack);
            return NULL;
         }
     }  

     mysql_free_result(res);
     mysql_close(conn); 

     return stack;
}

char* lurk_mysql_get_path(const char *filename)
{

    MYSQL *conn;
    MYSQL_RES *res;
    MYSQL_ROW row;

    int len=0;
    int ret=0;

    char *newpath, *qfilename;
    
    char query[MAXQUERY];

    if (filename == NULL)
        return NULL;

    if ((conn = lurk_mysql_get_connect()) == NULL)
        return NULL;

    qfilename = mymalloc((strlen(filename) * 2) + 1);
    mysql_real_escape_string(conn, qfilename, filename, strlen(filename));
   
    if ((strlen(qfilename) + 52) >= MAXQUERY) {
       fprintf(stderr, "QUERY TOO LONG\n");
       free(qfilename);
       mysql_close(conn);
       return NULL;
    }
    
    memset(query, '\0', MAXQUERY);  
    sprintf(query, "select path from filepaths where filename='%s' limit 1", qfilename);

    if ((ret = mysql_query(conn, query)) != 0) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        free(qfilename);
        mysql_close(conn);
        return NULL;
    }
    
    free(qfilename);

    res = mysql_use_result(conn);
   
    if ((row = mysql_fetch_row(res)) != NULL) {
        len = strlen(row[0]);
        newpath = mymalloc(len+1);
        strncpy(newpath, row[0], len);
    }
    else {
        newpath = NULL;
    }

    mysql_free_result(res);
    mysql_close(conn);
    
    return newpath;
}      

char* get_last_slash(char *path) 
{
   
    char *t;
    char *temp = NULL;

    t = strtok(path, "/");
 
    while(t != NULL) {
       temp = t;
       t = strtok(NULL, "/");
    } 

    return temp;
} 
 
int lurk_path_parse(const char *path)
{

    lurk_path_t lurkv;

    int ret=-1;
    size_t rm;
    regex_t sd_preg, sdd_preg, nr_preg, file_preg;
    regmatch_t pmatch[1];

    char *series_dir_match = "/Series/[0-9A-Z\\-]+/.*?/?$";
    char *series_ddir_match = "/Series/[0-9A-Z\\-]+/?$";
    char *nr_file_match = "/New_Releases/.*?$";
    char *file_match = "/Series/[0-9A-Z\\-]+/[[:alnum:][:punct:]\\ ^/]+/[[:alnum:][:punct:]^/]+$";

    if (strcmp(path, "/") == 0) {
       ret = lurkv=ROOTDIR;
    }
    else if ((strcmp(path, "/Series/") == 0) || (strcmp(path, "/Series") == 0)) {
       ret = lurkv=BROWSEDIR;
    } 
    else if ((strcmp(path, "/New_Releases/") == 0) || (strcmp(path, "/New_Releases") == 0)) {
       ret = lurkv=NRDIR;
    } 
    else {

       if ((rm = regcomp(&sd_preg, series_dir_match, REG_EXTENDED)) != 0)  {
          fprintf(stderr," Invalid expression:'%s'\n", series_dir_match);
          return -1;
       }

       if ((rm = regcomp(&nr_preg, nr_file_match, REG_EXTENDED)) != 0)  {
          fprintf(stderr, "Invalid expression:'%s'\n", nr_file_match);
          return -1;
       } 

       if ((rm = regcomp(&sdd_preg, series_ddir_match, REG_EXTENDED)) != 0)  {
          fprintf(stderr, "Invalid expression:'%s'\n", series_ddir_match);
          return -1;
       }
 
       if ((rm = regcomp(&file_preg, file_match, REG_EXTENDED)) != 0)  {
          fprintf(stderr, "Invalid expression:'%s'\n", file_match);
          return -1;
       }

       if ((rm = regexec(&sdd_preg, path, 1, pmatch, 0)) == 0) { 
          ret = lurkv=SERIESROOTDIR;
       }  
       else if ((rm = regexec(&file_preg, path, 1, pmatch, 0)) == 0) { 
          ret = lurkv=LFILE;
       }  
       else if ((rm = regexec(&sd_preg, path, 1, pmatch, 0)) == 0) { 
          ret = lurkv=SERIESDIR;
       }  
       else if ((rm = regexec(&nr_preg, path, 1, pmatch, 0)) == 0) { 
          ret = lurkv=LFILE;
       }
       regfree(&sdd_preg);
       regfree(&sd_preg);
       regfree(&nr_preg);
       regfree(&file_preg);
       
    }
    
    return ret;

} 

stack_t* lurk_get_dirs(const lurk_path_t lurkv, const char *path)
{

    int n=0;
     
    stack_t *cur = NULL;
    char *temp;
    char upath[MAXPATH];

    char *root[] = {"Series", "New_Releases", NULL};
    char *series_root[] = {"0-9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
                       "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", NULL};

    switch (lurkv) {

       case ROOTDIR:
          while (root[n] != NULL) {
             cur = push(cur, root[n]);
             n++;
          }
          break;
       case BROWSEDIR:
          while (series_root[n] != NULL) {
            cur = push(cur, series_root[n]);
            n++;
          } 
          break;
       case SERIESROOTDIR:
          if (strlen(path) >= MAXPATH)
              return NULL;
          strcpy(upath, path); 
          temp = get_last_slash(upath);
          if (temp != NULL) {
             cur = lurk_mysql_get_series(temp);
          }
          break;
       default:
          break;
    }
    
    return cur;
}

static int lurk_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{

    (void) offset;
    (void) fi;
  
    stack_t *cur;    

    lurk_path_t lurkv;

    char *temp;
    char upath[MAXPATH];

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
 
    lurkv = lurk_path_parse(path);

    if ((lurkv == ROOTDIR) || (lurkv == BROWSEDIR) || (lurkv == SERIESROOTDIR)) {
       cur = lurk_get_dirs(lurkv, path);
       while (cur != NULL) {
          struct stat st;
          memset(&st, 0, sizeof(st));
          st.st_mode = S_IFDIR | 0744;
          st.st_nlink = 2;
          if (filler(buf, cur->path, &st, 0))
             break;
          cur = pop(cur);
       }
    } 
    else if ((lurkv == SERIESDIR) || (lurkv == NRDIR)) {

       if (lurkv == SERIESDIR) {

          if (strlen(path) >= MAXPATH)
             return -1;
    
          strcpy(upath, path);
          temp = get_last_slash(upath);

          if (temp != NULL) {
             printf("%s\n", temp);
             cur = lurk_mysql_get_files(temp);
          } 
       }
       else if (lurkv == NRDIR) {
           cur = lurk_mysql_get_nr_files();
       }

       memset(upath, '\0', MAXPATH);

       while (cur != NULL) {
          struct stat st;
          memset(&st, 0, sizeof(st));
          stat(cur->path, &st); 
    
          strcpy(upath, cur->path);
          temp = get_last_slash(upath);

          if ((temp != NULL)) {
             if (filler(buf, temp, &st, 0))
                break;
          }
          cur = pop(cur);
       }
    }

    if (cur != NULL)
       free_stack(cur);

    return 0;
}

static int lurk_getattr(const char *path, struct stat *stbuf)
{
    int res=0;
   
    lurk_path_t lurkv;

    char *realpath, *temp;
    char upath[MAXPATH];

    lurkv = lurk_path_parse(path);

    if ((lurkv == ROOTDIR) || (lurkv == BROWSEDIR) || (lurkv == SERIESROOTDIR) || (lurkv == NRDIR) || (lurkv == SERIESDIR)) {
       memset(stbuf, 0, sizeof(struct stat));
       stbuf->st_mode = S_IFDIR | 0744;
       stbuf->st_nlink = 2;
    } 
    else if (lurkv == LFILE) { 

       if (strlen(path) >= MAXPATH)
          return -1;
    
       strcpy(upath, path); 
       temp = get_last_slash(upath);
       realpath = lurk_mysql_get_path(temp);

       if (realpath != NULL) {   
          res = stat(realpath, stbuf);
          free(realpath);
       }
       else {
         res = stat(path, stbuf);
       }
    }
    else {
       res = stat(path, stbuf);
    }
 
    if (res == -1)
       return -errno;
    
    return 0;
}

static int lurk_read(const char *path, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
    int fd;
    int res;

    (void) fi;

    char *realpath, *temp;
    char upath[MAXPATH];  

    if (strlen(path) >= MAXPATH)
       return -1;
    
    strcpy(upath, path); 
    temp = get_last_slash(upath);

    realpath = lurk_mysql_get_path(temp);

    if (realpath == NULL)
       return -1;

    /* check if user can open the file ~*/
    fd = open(realpath, O_RDONLY);
    free(realpath);
    if (fd == -1)
        return -errno;

    /* read specified amout of data ~*/
    res = pread(fd, buf, size, offset);
    if (res == -1)
        res = -errno;

    /* close file and return amount of bytes read */
    close(fd);
    return res;
}

static int lurk_open(const char *path, struct fuse_file_info *fi)
{
    int res;

    char *realpath, *temp;
    char upath[MAXPATH];

    if (strlen(path) >= MAXPATH)
       return -1;
    
    strcpy(upath, path);

    temp = get_last_slash(upath);
    realpath = lurk_mysql_get_path(temp);

    if (realpath == NULL)
       return -1;

    res = open(realpath, fi->flags);
    free(realpath);
    if (res == -1)
        return -errno;

    close(res);
    return 0;
}


static struct fuse_operations lurk_oper = {
    .getattr   = lurk_getattr,
    .readdir = lurk_readdir,
    .open   = lurk_open,
    .read   = lurk_read,
};
  
int main(int argc, char *argv[])
{
   return fuse_main(argc, argv, &lurk_oper, NULL);
}
