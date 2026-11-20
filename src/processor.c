
//processor.c
//the logic for this is that it consumes the csv chunks from the fifo that came from ingester and processes them using a bounded thread pool
//for each symbol, it aggrgates the tick data into vwap, high,low, and total volume
//it writes the aggrgated results to the posix shared memory and then posts to a named semaphore to unblock the reporter.
//the concurrency layout is shown:
 /*
   reader thread   : pulls ChunkHeader+payload from FIFO, enqueues QueueItem
   N worker threads: dequeue QueueItem, parse CSV rows, update agg table
 
   Semaphores  : sem_empty (init=Q), sem_full (init=0)  – bounded buffer
   Mutex       : queue_mutex  – protects queue head/tail
   Mutex       : agg_mutex    – protects aggregation table
 */

#include "common/common.h"

//queue item
typedef struct {
    char  *data;        //this a heap-allocated chunk payload
    int    byte_count;
    int    is_poison;   // 1 means the worker should exit
} QueueItem;

// Globals set from argv 
static int         g_num_threads  = 4;
static int         g_queue_size   = 16;
static const char *g_fifo_path    = NULL;
static const char *g_shm_name     = NULL;
static const char *g_sem_name     = NULL;

// Bounded queue
static QueueItem      *g_queue      = NULL;
static int             g_q_head     = 0;
static int             g_q_tail     = 0;
static pthread_mutex_t g_q_mutex    = PTHREAD_MUTEX_INITIALIZER;
static sem_t           g_sem_empty;   /* unnamed, in-process */
static sem_t           g_sem_full;

// Aggregation table 
static TickEntry       g_agg[MAX_SYMBOLS];
static int             g_agg_count  = 0;
static pthread_mutex_t g_agg_mutex  = PTHREAD_MUTEX_INITIALIZER;

//live stats
static volatile long   g_rows_processed = 0;
static volatile sig_atomic_t g_dump_stats = 0;

//signal handler
static void handle_sigusr1(int sig) { (void)sig; g_dump_stats = 1; }

//this finds or creates an aggregation slot for a symbol
static TickEntry *get_or_create_entry(const char *symbol) {
    // search existing
    for (int i = 0; i < g_agg_count; i++) {
        if (g_agg[i].valid && strcmp(g_agg[i].symbol, symbol) == 0)
            return &g_agg[i];
    }
    //this creates new slot
    if (g_agg_count >= MAX_SYMBOLS) return NULL;
    TickEntry *e = &g_agg[g_agg_count++];
    memset(e, 0, sizeof(*e));
    strncpy(e->symbol, symbol, MAX_SYMBOL_LEN - 1);
    e->high  = -1e18;   // will be updated on first row
    e->low   =  1e18;
    e->valid = 1;
    return e;
}

//now this follwing functions parses one CSV row and updates the aggregation table
//the expected format is: symbol,price,volume
//e.g aapl,150.5,200
 
static void process_row(const char *line) {
    char buf[MAX_LINE_LEN];
    strncpy(buf, line, MAX_LINE_LEN - 1);
    buf[MAX_LINE_LEN - 1] = '\0';

    //this strips trailing newline
    size_t l = strlen(buf);
    while (l > 0 && (buf[l-1] == '\n' || buf[l-1] == '\r')) buf[--l] = '\0';
    if (l == 0) return;

    char *tok = strtok(buf, ",");
    if (!tok) return;
    char symbol[MAX_SYMBOL_LEN];
    strncpy(symbol, tok, MAX_SYMBOL_LEN - 1);
    symbol[MAX_SYMBOL_LEN - 1] = '\0';

    tok = strtok(NULL, ",");
    if (!tok) return;
    double price = atof(tok);

    tok = strtok(NULL, ",");
    if (!tok) return;
    double volume = atof(tok);

    if (volume <= 0 || price <= 0) return;

    pthread_mutex_lock(&g_agg_mutex);
    TickEntry *e = get_or_create_entry(symbol);
    if (e) {
        e->sum_pv      += price * volume;
        e->sum_v       += volume;
        e->total_volume += volume;
        if (price > e->high) e->high = price;
        if (price < e->low)  e->low  = price;
    }
    pthread_mutex_unlock(&g_agg_mutex);

    __sync_fetch_and_add(&g_rows_processed, 1);
}

//this processes a full chunk (multiple CSV rows)
static void process_chunk(const char *data, int byte_count) {
    char buf[MAX_CHUNK_BYTES + 1];
    int  copy = byte_count < MAX_CHUNK_BYTES ? byte_count : MAX_CHUNK_BYTES;
    memcpy(buf, data, copy);
    buf[copy] = '\0';

    char *saveptr = NULL;
    char *line    = strtok_r(buf, "\n", &saveptr);
    while (line) {
        process_row(line);
        line = strtok_r(NULL, "\n", &saveptr);
    }
}

//this is a worker thread function 
static void *worker_func(void *arg) {
    (void)arg;
    LOG("Worker thread started");

    for (;;) {
        sem_wait(&g_sem_full);

        pthread_mutex_lock(&g_q_mutex);
        QueueItem item = g_queue[g_q_head];
        g_q_head = (g_q_head + 1) % g_queue_size;
        pthread_mutex_unlock(&g_q_mutex);

        sem_post(&g_sem_empty);

        if (item.is_poison) {
            LOG("Worker received poison pill, exiting");
            break;
        }

        process_chunk(item.data, item.byte_count);
        free(item.data);
    }
    return NULL;
}

//this reads exactly n bytes from fd
static int read_all(int fd, void *buf, size_t n) {
    char   *p         = (char *)buf;
    size_t  remaining = n;
    while (remaining > 0) {
        ssize_t r = read(fd, p, remaining);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) return -1;   // EOF — unexpected
        p         += r;
        remaining -= (size_t)r;
    }
    return 0;
}

static void enqueue(QueueItem item) {
    sem_wait(&g_sem_empty);
    pthread_mutex_lock(&g_q_mutex);
    g_queue[g_q_tail] = item;
    g_q_tail = (g_q_tail + 1) % g_queue_size;
    pthread_mutex_unlock(&g_q_mutex);
    sem_post(&g_sem_full);
}

//this is the reader thread( pulls from FIFO and enqueues items) 
static void *reader_func(void *arg) {
    int fifo_fd = *(int *)arg;
    LOG("Reader thread started");

    for (;;) {
        if (g_dump_stats) {
            fprintf(stderr,
                "[PID=%d PPID=%d] SIGUSR1: rows_processed=%ld\n",
                (int)getpid(), (int)getppid(), g_rows_processed);
            g_dump_stats = 0;
        }

        ChunkHeader hdr;
        if (read_all(fifo_fd, &hdr, sizeof(hdr)) < 0) {
            LOG("Reader: FIFO closed unexpectedly");
            break;
        }

        if (hdr.is_eof) {
            LOG("Reader: received EOF chunk");
            break;
        }

        //this reads payload
        char *payload = (char *)malloc(hdr.byte_count + 1);
        if (!payload) { LOG("Reader: malloc failed"); break; }

        if (read_all(fifo_fd, payload, hdr.byte_count) < 0) {
            free(payload);
            LOG("Reader: failed to read chunk payload");
            break;
        }
        payload[hdr.byte_count] = '\0';

        LOG("Reader: chunk_id=%d file=%d bytes=%d",
            hdr.chunk_id, hdr.source_file_id, hdr.byte_count);

        QueueItem item = { payload, hdr.byte_count, 0 };
        enqueue(item);
    }

    //this sends one poison pill per worker thread
    for (int i = 0; i < g_num_threads; i++) {
        QueueItem poison = { NULL, 0, 1 };
        enqueue(poison);
    }
    LOG("Reader: sent %d poison pills", g_num_threads);
    return NULL;
}

//this serialises aggregation table into shared memory
static void write_shared_memory(void) {
    // open / create shared memory segment
    int shm_fd = shm_open(g_shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG("ERROR: shm_open failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    if (ftruncate(shm_fd, sizeof(SharedData)) < 0) {
        LOG("ERROR: ftruncate failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }

    SharedData *shm = (SharedData *)mmap(NULL, sizeof(SharedData),
                                          PROT_READ | PROT_WRITE,
                                          MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        LOG("ERROR: mmap failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }

    shm->count = g_agg_count;
    memcpy(shm->entries, g_agg, sizeof(TickEntry) * g_agg_count);

    munmap(shm, sizeof(SharedData));
    close(shm_fd);
    LOG("Aggregation table (%d symbols) written to shared memory", g_agg_count);
}

//Main
int main(int argc, char *argv[]) {
    if (argc < 6) {
        fprintf(stderr,
            "Usage: processor <fifo_path> <shm_name> <sem_name> "
            "<num_threads> <queue_size>\n");
        exit(EXIT_BAD_ARGS);
    }

    g_fifo_path   = argv[1];
    g_shm_name    = argv[2];
    g_sem_name    = argv[3];
    g_num_threads = atoi(argv[4]);
    g_queue_size  = atoi(argv[5]);

    if (g_num_threads < 1 || g_queue_size < 1) {
        fprintf(stderr, "ERROR: num_threads and queue_size must be >= 1\n");
        exit(EXIT_BAD_ARGS);
    }

    //installing signal handler
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigusr1;
    sigaction(SIGUSR1, &sa, NULL);

    LOG("Processor started. threads=%d queue=%d fifo=%s shm=%s sem=%s",
        g_num_threads, g_queue_size, g_fifo_path, g_shm_name, g_sem_name);

    //now we initialise semaphores
    if (sem_init(&g_sem_empty, 0, (unsigned)g_queue_size) < 0 ||
        sem_init(&g_sem_full,  0, 0) < 0) {
        LOG("ERROR: sem_init failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }

    //here we are allocatating bounded queue
    g_queue = (QueueItem *)calloc((size_t)g_queue_size, sizeof(QueueItem));
    if (!g_queue) { LOG("ERROR: calloc failed"); exit(EXIT_IPC_FAIL); }

    //opening FIFO for reading 
    int fifo_fd = open(g_fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        LOG("ERROR: open FIFO failed: %s", strerror(errno));
        exit(EXIT_IO_ERROR);
    }
    LOG("FIFO opened for reading");

    //creating worker threads with explicit attributes
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 1024 * 1024);  /* 1 MB stack per worker */

    pthread_t *workers = (pthread_t *)malloc(
                            (size_t)g_num_threads * sizeof(pthread_t));
    if (!workers) { LOG("ERROR: malloc workers"); exit(EXIT_IPC_FAIL); }

    for (int i = 0; i < g_num_threads; i++) {
        if (pthread_create(&workers[i], &attr, worker_func, NULL) != 0) {
            LOG("ERROR: pthread_create failed: %s", strerror(errno));
            exit(EXIT_IPC_FAIL);
        }
    }
    pthread_attr_destroy(&attr);

    //creating reader thread
    pthread_t reader_tid;
    pthread_attr_t reader_attr;
    pthread_attr_init(&reader_attr);
    pthread_attr_setdetachstate(&reader_attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&reader_attr, 512 * 1024);  /* 512 KB */
    pthread_create(&reader_tid, &reader_attr, reader_func, &fifo_fd);
    pthread_attr_destroy(&reader_attr);

    //this waits for the reader to finish
    pthread_join(reader_tid, NULL);
    LOG("Reader thread joined");

    //this waits for all the workers to finish
    for (int i = 0; i < g_num_threads; i++) {
        pthread_join(workers[i], NULL);
        LOG("Worker %d joined", i);
    }

    close(fifo_fd);

    // cleanup queue
    free(g_queue);
    free(workers);
    sem_destroy(&g_sem_empty);
    sem_destroy(&g_sem_full);

    LOG("All workers done. Total rows processed: %ld", g_rows_processed);

    // write results to shared memory
    write_shared_memory();

    //posting named semaphore to wake reporter
    sem_t *sem_ready = sem_open(g_sem_name, O_CREAT, 0666, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("ERROR: sem_open failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    sem_post(sem_ready);
    sem_close(sem_ready);
    LOG("Named semaphore posted — reporter can now proceed");

    exit(EXIT_SUCCESS_CODE);
}
