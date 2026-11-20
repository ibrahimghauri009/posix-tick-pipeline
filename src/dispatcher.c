
 //dispatcher.c . what this does is it parses command-line arguments(the ones we use on terminal).
 //secondly, it creates fifo and posix shared-memory segment(so that the processor and reporter can share data without using pipe)
 // thirdly, it creates a named semaphore which is used by processor-> reporter
 //forthy, it forks and execs the three child processes(ingester,processor and reporter), redirecting edch child's stout to a per-process log file
 //it also blockes in sisuspend watiign for children or a shutdown signal
 // on shutdown, it sigterms children and unlinks all ipc

#include "common/common.h"

//Globals
static pid_t  g_pid_ingester  = -1;
static pid_t  g_pid_processor = -1;
static pid_t  g_pid_reporter  = -1;

static const char *g_fifo_path  = FIFO_PATH;
static const char *g_shm_name   = SHM_NAME;
static const char *g_sem_name   = SEM_READY_NAME;
static const char *g_input_dir  = NULL;
static const char *g_output_dir = NULL;
static int         g_num_threads = 4;
static int         g_queue_size  = 16;

static volatile sig_atomic_t g_got_sigint    = 0;
static volatile sig_atomic_t g_got_sigterm   = 0;
static volatile sig_atomic_t g_got_sigchld   = 0;
static volatile sig_atomic_t g_got_sigusr1   = 0;   //reporter done

static time_t g_start_time;

//Signal Handlers
static void handle_sigint(int s)  { (void)s; g_got_sigint  = 1; }
static void handle_sigterm(int s) { (void)s; g_got_sigterm = 1; }
static void handle_sigchld(int s) { (void)s; g_got_sigchld = 1; }
static void handle_sigusr1(int s) { (void)s; g_got_sigusr1 = 1; }



//this is called inside the child after the fork and before exec. it opens logs and redirects both stoud and sterr into it.
//it also demonstrates saving descriptor with dup() before replacing it.
 
static void redirect_child_logs(const char *child_name) {
    //ensure log directory exists
    mkdir(LOG_DIR, 0755);

    char log_path[256];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, child_name);

    int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (log_fd < 0) {
        fprintf(stderr, "WARNING: cannot open log %s: %s\n",
                log_path, strerror(errno));
        return;
    }

    // save original stderr with dup() before we replace it
    int saved_stderr = dup(STDERR_FILENO);   //demonstration of dup()

    dup2(log_fd, STDOUT_FILENO);
    dup2(log_fd, STDERR_FILENO);
    close(log_fd);

    // saved_stderr is now unused in the child, close it
    if (saved_stderr >= 0) close(saved_stderr);
}

//Fork + exec one child:
static pid_t spawn_child(const char *name, char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        LOG("ERROR: fork() failed for %s: %s", name, strerror(errno));
        return -1;
    }
    if (pid == 0) {
        // child
        redirect_child_logs(name);
        execvp(argv[0], argv);
        // if we reach here exec failed
        fprintf(stderr, "ERROR: execvp(%s) failed: %s\n",
                argv[0], strerror(errno));
        _exit(EXIT_IO_ERROR);
    }
    //parent
    LOG("Spawned %s with PID %d", name, (int)pid);
    return pid;
}

// Forward SIGTERM to all living children 
static void terminate_children(void) {
    if (g_pid_ingester  > 0) kill(g_pid_ingester,  SIGTERM);
    if (g_pid_processor > 0) kill(g_pid_processor, SIGTERM);
    if (g_pid_reporter  > 0) kill(g_pid_reporter,  SIGTERM);
}

//this cleans up all IPC resources

static void cleanup_ipc(void) {
    unlink(g_fifo_path);
    shm_unlink(g_shm_name);
    sem_unlink(g_sem_name);
    LOG("IPC resources unlinked");
}

//usage
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s -i <input_dir> -o <output_dir> "
        "[-n <threads>] [-q <queue_size>] "
        "[-f <fifo_path>] [-s <shm_name>] [-e <sem_name>]\n",
        prog);
}


int main(int argc, char *argv[]) {
    g_start_time = time(NULL);

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "i:o:n:q:f:s:e:")) != -1) {
        switch (opt) {
            case 'i': g_input_dir   = optarg;        break;
            case 'o': g_output_dir  = optarg;        break;
            case 'n': g_num_threads = atoi(optarg);  break;
            case 'q': g_queue_size  = atoi(optarg);  break;
            case 'f': g_fifo_path   = optarg;        break;
            case 's': g_shm_name    = optarg;        break;
            case 'e': g_sem_name    = optarg;        break;
            default:  usage(argv[0]); exit(EXIT_BAD_ARGS);
        }
    }

    if (!g_input_dir || !g_output_dir) {
        fprintf(stderr, "ERROR: -i and -o are required\n");
        usage(argv[0]);
        exit(EXIT_BAD_ARGS);
    }
    if (g_num_threads < 1 || g_queue_size < 1) {
        fprintf(stderr, "ERROR: threads and queue size must be >= 1\n");
        exit(EXIT_BAD_ARGS);
    }

    //ensure output dir exists
    mkdir(g_output_dir, 0755);

    LOG("Dispatcher started. input=%s output=%s threads=%d queue=%d",
        g_input_dir, g_output_dir, g_num_threads, g_queue_size);

    //this installs signal handlers
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));

    sa.sa_handler = handle_sigint;
    sigaction(SIGINT,  &sa, NULL);

    sa.sa_handler = handle_sigterm;
    sigaction(SIGTERM, &sa, NULL);

    sa.sa_handler = handle_sigchld;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = handle_sigusr1;
    sa.sa_flags   = 0;
    sigaction(SIGUSR1, &sa, NULL);

    //creating FIFO here
    unlink(g_fifo_path);   //remove stale FIFO if any
    if (mkfifo(g_fifo_path, 0666) < 0) {
        LOG("ERROR: mkfifo(%s) failed: %s", g_fifo_path, strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    LOG("FIFO created: %s", g_fifo_path);

    //pre-creating shared memory segment
    shm_unlink(g_shm_name);
    int shm_fd = shm_open(g_shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG("ERROR: shm_open failed: %s", strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    ftruncate(shm_fd, sizeof(SharedData));
    close(shm_fd);
    LOG("Shared memory segment created: %s", g_shm_name);

    // Pre-create named semaphore
    sem_unlink(g_sem_name);
    sem_t *sem = sem_open(g_sem_name, O_CREAT | O_EXCL, 0666, 0);
    if (sem == SEM_FAILED) {
        LOG("ERROR: sem_open(%s) failed: %s", g_sem_name, strerror(errno));
        exit(EXIT_IPC_FAIL);
    }
    sem_close(sem);
    LOG("Named semaphore created: %s", g_sem_name);

    // Spawn ingester
    char *ingester_argv[] = {
        "./ingester",
        (char *)g_input_dir,
        (char *)g_fifo_path,
        NULL
    };
    g_pid_ingester = spawn_child("ingester", ingester_argv);
    if (g_pid_ingester < 0) { cleanup_ipc(); exit(EXIT_IPC_FAIL); }

    // Spawn processor
    char threads_str[16], queue_str[16];
    snprintf(threads_str, sizeof(threads_str), "%d", g_num_threads);
    snprintf(queue_str,   sizeof(queue_str),   "%d", g_queue_size);

    char *processor_argv[] = {
        "./processor",
        (char *)g_fifo_path,
        (char *)g_shm_name,
        (char *)g_sem_name,
        threads_str,
        queue_str,
        NULL
    };
    g_pid_processor = spawn_child("processor", processor_argv);
    if (g_pid_processor < 0) { cleanup_ipc(); exit(EXIT_IPC_FAIL); }

    // Spawn reporter
    char disp_pid_str[16];
    snprintf(disp_pid_str, sizeof(disp_pid_str), "%d", (int)getpid());

    char *reporter_argv[] = {
        "./reporter",
        (char *)g_shm_name,
        (char *)g_sem_name,
        (char *)g_output_dir,
        disp_pid_str,
        NULL
    };
    g_pid_reporter = spawn_child("reporter", reporter_argv);
    if (g_pid_reporter < 0) { cleanup_ipc(); exit(EXIT_IPC_FAIL); }

    //Wait loop is using sigsuspend (no busy-wait)
    LOG("All children spawned. Waiting (sigsuspend loop) ...");

    sigset_t mask_all, mask_orig;
    sigfillset(&mask_all);
    sigprocmask(SIG_BLOCK, &mask_all, &mask_orig);

    int children_alive = 3;
    int exit_code      = EXIT_SUCCESS_CODE;

    while (children_alive > 0 && !g_got_sigint && !g_got_sigterm) {
        sigsuspend(&mask_orig);

        //SIGCHLD: reap whichever child died
        if (g_got_sigchld) {
            g_got_sigchld = 0;
            int status = 0;
            pid_t dead;
            while ((dead = waitpid(-1, &status, WNOHANG)) > 0) {
                time_t elapsed = time(NULL) - g_start_time;
                const char *cname = "unknown";
                if (dead == g_pid_ingester)  { cname = "ingester";  g_pid_ingester  = -1; }
                if (dead == g_pid_processor) { cname = "processor"; g_pid_processor = -1; }
                if (dead == g_pid_reporter)  { cname = "reporter";  g_pid_reporter  = -1; }
                children_alive--;

                if (WIFEXITED(status))
                    LOG("Child %-12s PID=%d exited with status=%d runtime=%lds",
                        cname, (int)dead, WEXITSTATUS(status), (long)elapsed);
                else if (WIFSIGNALED(status)) {
                    LOG("Child %-12s PID=%d killed by signal=%d runtime=%lds",
                        cname, (int)dead, WTERMSIG(status), (long)elapsed);
                    exit_code = EXIT_CHILD_DIED;
                }
            }
        }

        //SIGUSR1: reporter signals it's done
        if (g_got_sigusr1) {
            g_got_sigusr1 = 0;
            LOG("SIGUSR1 received from reporter — report is ready");
        }
    }

    // Shutdown path (SIGINT / SIGTERM)
    if (g_got_sigint || g_got_sigterm) {
        LOG("Shutdown signal received — terminating children");
        terminate_children();

        // give children 3 seconds to exit cleanly 
        sleep(3);

        // reap any remaining children
        pid_t dead;
        int status;
        while ((dead = waitpid(-1, &status, WNOHANG)) > 0) {
            LOG("Reaped child PID=%d after shutdown", (int)dead);
        }

        exit_code = g_got_sigint ? EXIT_SIGINT : EXIT_SIGTERM;
    }

    sigprocmask(SIG_SETMASK, &mask_orig, NULL);

    //Cleanup IPC
    cleanup_ipc();

    time_t total_runtime = time(NULL) - g_start_time;
    LOG("Dispatcher exiting. runtime=%lds exit_code=%d",
        (long)total_runtime, exit_code);

    exit(exit_code);
}
