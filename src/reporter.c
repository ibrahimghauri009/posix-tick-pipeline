//reporter.c
//what this does is it blocks on the named posix semaphore signaled by the processor
//it then retrieves the aggrgation table for the shared memory and then generated the report.txt file for human readable output
//it also created report.csv for machine-readible summary using dup.
//once the writing is finished, it notifies the dispatcher by sending the sigusr1 to its parent process

#include "common/common.h"

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr,
            "Usage: reporter <shm_name> <sem_name> <output_dir> <dispatcher_pid>\n");
        exit(EXIT_BAD_ARGS);
    }

    const char *shm_name    = argv[1];
    const char *sem_name    = argv[2];
    const char *output_dir  = argv[3];
    pid_t       disp_pid    = (pid_t)atoi(argv[4]);

    LOG("Reporter started. shm=%s sem=%s out=%s dispatcher_pid=%d",
        shm_name, sem_name, output_dir, (int)disp_pid);

    //step 1. we wait on named semaphore (no busy-wait)
    sem_t *sem_ready = sem_open(sem_name, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("ERROR: sem_open(%s) failed: %s", sem_name, strerror(errno));
        exit(EXIT_IPC_FAIL);
    }

    LOG("Waiting on semaphore %s ...", sem_name);
    if (sem_wait(sem_ready) < 0) {
        LOG("ERROR: sem_wait failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    sem_close(sem_ready);
    LOG("Semaphore acquired — aggregation data is ready");

    // step 2. we map the shared memory
    int shm_fd = shm_open(shm_name, O_RDONLY, 0666);
    if (shm_fd < 0) {
        LOG("ERROR: shm_open(%s) failed: %s", shm_name, strerror(errno));
        exit(EXIT_IPC_FAIL);
    }

    SharedData *shm = (SharedData *)mmap(NULL, sizeof(SharedData),
                                          PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm == MAP_FAILED) {
        LOG("ERROR: mmap failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    close(shm_fd);
    LOG("Shared memory mapped. Symbol count = %d", shm->count);

    //step3.we build the output file paths 
    char txt_path[512], csv_path[512];
    snprintf(txt_path, sizeof(txt_path), "%s/report.txt", output_dir);
    snprintf(csv_path, sizeof(csv_path), "%s/report.csv", output_dir);

    //step 4. we write the report.txt using dup() / dup2()
    
    /*
      dup/dup2 demonstration per project requirements:
      
      Step A: Preserve the existing stdout file descriptor by calling dup(): 
      saved_stdout = dup(STDOUT_FILENO), which creates a duplicate descriptor 
      referring to the same open file, typically the terminal.
      
      Step B: Open report.txt and redirect stdout to it with dup2(report_fd, STDOUT_FILENO), 
      causing subsequent printf/puts calls to write to report.txt instead of the terminal.
      
      Step C: Emit the human-readable report using standard printf() calls.
      
      Step D: Restore the  original stdout by executing dup2(saved_stdout, STDOUT_FILENO)
      and close saved_stdout since it is no longer required.
     */
    //follwoing are the steps for the dup/dup2 demonstration as mentioned in the requiremnts
    //step A: we save stdout
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout < 0) {
        LOG("ERROR: dup(stdout) failed: %s", strerror(errno));
        exit(EXIT_IO_ERROR);
    }

    //step B: we open report.txt and redirect stdout
    int report_fd = open(txt_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (report_fd < 0) {
        LOG("ERROR: open(%s) failed: %s", txt_path, strerror(errno));
        exit(EXIT_IO_ERROR);
    }
    if (dup2(report_fd, STDOUT_FILENO) < 0) {
        LOG("ERROR: dup2 to report.txt failed: %s", strerror(errno));
        exit(EXIT_IO_ERROR);
    }
    close(report_fd);   // original fd no longer needed

    //step C:we write the human-readable report via printf (goes to file)
    time_t now = time(NULL);
    char   time_buf[64];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime(&now));

    printf("-------------------------------------------------------------\n");
    printf("  FINANCIAL TICK DATA — AGGREGATION REPORT\n");
    printf("  Generated : %s\n", time_buf);
    printf("  Symbols   : %d\n", shm->count);
    printf("-------------------------------------------------------------\n\n");

    printf("%-12s  %10s  %10s  %10s  %15s\n",
       "SYMBOL", "HIGH", "LOW", "VWAP", "TOTAL_VOLUME");
    printf("%-12s  %10s  %10s  %10s  %15s\n",
       "------------", "----------", "----------", "----------",
       "---------------");

    for (int i = 0; i < shm->count; i++) {
        const TickEntry *e = &shm->entries[i];
        if (!e->valid) continue;
        double vwap = (e->sum_v > 0) ? (e->sum_pv / e->sum_v) : 0.0;
        printf("%-12s  %10.2f  %10.2f  %10.4f  %15.0f\n",
               e->symbol, e->high, e->low, vwap, e->total_volume);
    }

    printf("\n---------------------------------------------------------------\n");
    printf("  END OF REPORT\n");
    printf("---------------------------------------------------------------\n");
    fflush(stdout);

    //step D: we restore the stdout
    if (dup2(saved_stdout, STDOUT_FILENO) < 0) {
        // if we can't restore, we log to stderr and continue
        fprintf(stderr, "[PID=%d] ERROR: dup2 restore failed: %s\n",
                (int)getpid(), strerror(errno));
    }
    close(saved_stdout);

    LOG("report.txt written to %s", txt_path);

    // step 5.we write the report.csv
    FILE *csv = fopen(csv_path, "w");
    if (!csv) {
        LOG("ERROR: cannot open %s: %s", csv_path, strerror(errno));
        exit(EXIT_IO_ERROR);
    }

    fprintf(csv, "symbol,high,low,vwap,total_volume\n");
    for (int i = 0; i < shm->count; i++) {
        const TickEntry *e = &shm->entries[i];
        if (!e->valid) continue;
        double vwap = (e->sum_v > 0) ? (e->sum_pv / e->sum_v) : 0.0;
        fprintf(csv, "%s,%.2f,%.2f,%.4f,%.0f\n",
                e->symbol, e->high, e->low, vwap, e->total_volume);
    }
    fclose(csv);
    LOG("report.csv written to %s", csv_path);

    //step 6. we unmap the shared memory
    munmap(shm, sizeof(SharedData));

    //step 7. we signal the dispatcher that report is ready
    LOG("Sending SIGUSR1 to dispatcher (PID=%d)", (int)disp_pid);
    if (kill(disp_pid, SIGUSR1) < 0) {
        LOG("WARNING: kill(dispatcher, SIGUSR1) failed: %s", strerror(errno));
    }

    LOG("Reporter done successfully");
    exit(EXIT_SUCCESS_CODE);
}
