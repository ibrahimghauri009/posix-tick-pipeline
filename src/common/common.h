#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>

//exit Codes
#define EXIT_SUCCESS_CODE     0
#define EXIT_BAD_ARGS        10
#define EXIT_IPC_FAIL        20
#define EXIT_CHILD_DIED      30
#define EXIT_IO_ERROR        40
#define EXIT_SIGINT         130
#define EXIT_SIGTERM        143

//pipeline constants 
#define FIFO_PATH            "/tmp/os_project_fifo"
#define SHM_NAME             "/os_project_shm"
#define SEM_READY_NAME       "/os_project_sem_ready"
#define LOG_DIR              "logs"
#define MAX_SYMBOLS          256
#define MAX_SYMBOL_LEN        32
#define CHUNK_SIZE_ROWS      100   //this shows rows per chunk
#define MAX_LINE_LEN         256
#define MAX_CHUNK_BYTES     8192   //this gives max bytes in one chunk payload

//chunk header (prepended to every FIFO chunk)
typedef struct {
    int  chunk_id;
    int  source_file_id;
    int  byte_count;
    int  is_eof;          // 1 = poison/EOF chunk, 0 = normal
} ChunkHeader;

typedef struct {
    char   symbol[MAX_SYMBOL_LEN];
    double sum_pv;          // sum(price * volume)
    double sum_v;           // sum(volume)
    double high;
    double low;
    double total_volume;
    int    valid;           // 1 = slot is in use
} TickEntry;

//shared Memory Layout 
typedef struct {
    int       count;                    //number of valid entries
    TickEntry entries[MAX_SYMBOLS];
} SharedData;

//logging Macro (this prints PID and PPID on every line)
#define LOG(fmt, ...) \
    fprintf(stderr, "[PID=%d PPID=%d] " fmt "\n", \
            (int)getpid(), (int)getppid(), ##__VA_ARGS__)

#endif // COMMON_H
