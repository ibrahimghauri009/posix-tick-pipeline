//ingestor.c
// firstly, what i did was implemented the logic for it to scan the input firectory for csv files and read them in chunks
//secondly, it attaches a small label at the front before sending each chunk of rows and then writes it to fifo(pipe connecting ingester to processor)
//on sigterm it writes a special eof chunk and exits cleanly
//on sigusr1, it prints live statistics to sterr.

#include "common/common.h"

//globals for signal handler
static volatile sig_atomic_t g_terminate   = 0;
static volatile sig_atomic_t g_dump_stats  = 0;

//live stats
static int    g_files_processed = 0;
static int    g_chunks_sent     = 0;
static long   g_bytes_sent      = 0;

//signal Handlers 
static void handle_sigterm(int sig) { (void)sig; g_terminate  = 1; }
static void handle_sigusr1(int sig) { (void)sig; g_dump_stats = 1; }

//writes exactly n bytes to fd (retry on EINTR)
static int write_all(int fd, const void *buf, size_t n) {
    const char *p = (const char *)buf;
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = write(fd, p, remaining);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p         += w;
        remaining -= (size_t)w;
    }
    return 0;
}

//sends EOF/poison chunk so processor knows we are done
static void send_eof_chunk(int fifo_fd) {
    ChunkHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.is_eof = 1;
    write_all(fifo_fd, &hdr, sizeof(hdr));
    LOG("Sent EOF chunk to processor");
}

//processes one CSV file 
static void process_file(int fifo_fd, const char *filepath, int file_id) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) {
        LOG("ERROR: cannot open %s: %s", filepath, strerror(errno));
        return;
    }

    char    line_buf[MAX_LINE_LEN];
    char    chunk_buf[MAX_CHUNK_BYTES];
    int     chunk_id   = 0;
    int     row_count  = 0;
    size_t  chunk_pos  = 0;

    while (!g_terminate && fgets(line_buf, sizeof(line_buf), fp)) {
        //this skips blank lines
        size_t len = strlen(line_buf);
        if (len == 0 || line_buf[0] == '\n') continue;

        //this if condition states that will this line fit in the current chunk?
        if (chunk_pos + len >= MAX_CHUNK_BYTES || row_count >= CHUNK_SIZE_ROWS) {
            //this flushes current chunk
            if (chunk_pos > 0) {
                ChunkHeader hdr;
                hdr.chunk_id      = chunk_id++;
                hdr.source_file_id = file_id;
                hdr.byte_count    = (int)chunk_pos;
                hdr.is_eof        = 0;

                write_all(fifo_fd, &hdr,      sizeof(hdr));
                write_all(fifo_fd, chunk_buf, chunk_pos);

                g_chunks_sent++;
                g_bytes_sent += (long)(sizeof(hdr) + chunk_pos);

                chunk_pos = 0;
                row_count = 0;
            }
        }

        memcpy(chunk_buf + chunk_pos, line_buf, len);
        chunk_pos += len;
        row_count++;
    }

    //this flushes remaining rows
    if (chunk_pos > 0 && !g_terminate) {
        ChunkHeader hdr;
        hdr.chunk_id       = chunk_id++;
        hdr.source_file_id = file_id;
        hdr.byte_count     = (int)chunk_pos;
        hdr.is_eof         = 0;

        write_all(fifo_fd, &hdr,      sizeof(hdr));
        write_all(fifo_fd, chunk_buf, chunk_pos);

        g_chunks_sent++;
        g_bytes_sent += (long)(sizeof(hdr) + chunk_pos);
    }

    fclose(fp);
    g_files_processed++;
    LOG("Finished file %s (%d chunks)", filepath, chunk_id);
}

//main 
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: ingester <input_dir> <fifo_path>\n");
        exit(EXIT_BAD_ARGS);
    }

    const char *input_dir = argv[1];
    const char *fifo_path = argv[2];

    //installing signal handlers
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigterm;
    sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = handle_sigusr1;
    sigaction(SIGUSR1, &sa, NULL);

    LOG("Ingester started. input_dir=%s fifo=%s", input_dir, fifo_path);

    //opening FIFO for writing, and blocking until processor opens read end
    int fifo_fd = open(fifo_path, O_WRONLY);
    if (fifo_fd < 0) {
        LOG("ERROR: cannot open FIFO %s: %s", fifo_path, strerror(errno));
        exit(EXIT_IO_ERROR);
    }
    LOG("FIFO opened for writing");

    //this scans the input directory for *.csv files
    DIR *dir = opendir(input_dir);
    if (!dir) {
        LOG("ERROR: cannot open input dir %s: %s", input_dir, strerror(errno));
        close(fifo_fd);
        exit(EXIT_IO_ERROR);
    }

    int file_id = 0;
    struct dirent *entry;
    while (!g_terminate && (entry = readdir(dir)) != NULL) {
        //checking for SIGUSR1 dump
        if (g_dump_stats) {
            fprintf(stderr,
                "[PID=%d PPID=%d] SIGUSR1: files=%d chunks=%d bytes=%ld\n",
                (int)getpid(), (int)getppid(),
                g_files_processed, g_chunks_sent, g_bytes_sent);
            g_dump_stats = 0;
        }

        //this only processes csv files
        const char *name = entry->d_name;
        size_t nlen = strlen(name);
        if (nlen < 4 || strcmp(name + nlen - 4, ".csv") != 0) continue;

        //build full path
        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, name);

        LOG("Processing file [%d]: %s", file_id, filepath);
        process_file(fifo_fd, filepath, file_id);
        file_id++;
    }
    closedir(dir);

    //this sends EOF chunk to tell processor we are done
    send_eof_chunk(fifo_fd);
    close(fifo_fd);

    LOG("Ingester done. files=%d chunks=%d bytes=%ld",
        g_files_processed, g_chunks_sent, g_bytes_sent);

    exit(EXIT_SUCCESS_CODE);
}
