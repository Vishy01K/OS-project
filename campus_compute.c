/*
 * CampusCompute - Job Scheduling and Resource Management
 * Operating Systems (UE25CS242A) Mini-Project
 *
 * Build with: make
 * Run with: ./campuscompute help
 *
 * Available commands:
 * sched   - Run and compare CPU scheduling algorithms
 * logger  - Demonstrate the producer-consumer problem using threads
 * ipc     - Share data between processes using shared memory and semaphores
 * banker  - Check safe and unsafe resource allocation states
 * worker  - Create child processes and check their exit status
 * rlimit  - Demonstrate process resource limits
 * cleanup - Remove the IPC objects and log created by this program
 * demo    - Run all the features one after another
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_JOBS 32
#define MAX_SEG 1024
#define DEFAULT_QUANTUM 3
#define LOG_FILE "campuscompute.log"
#define SHM_NAME "/campuscompute_shm"
#define SEM_MUTEX "/campuscompute_mutex"
#define SEM_ITEMS "/campuscompute_items"
#define SEM_SLOTS "/campuscompute_slots"
#define IPC_SLOTS 4
#define MAXP 8
#define MAXR 8


/* Helper functions */
/* Read and validate command-line numbers. */
static int parse_long(const char *s, long lo, long hi, long *out)
{
    char *end;
    if (!s || !*s)
        return -1;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (*end != '\0' || errno != 0 || v < lo || v > hi)
        return -1;
    *out = v;
    return 0;
}

static void banner(const char *title)
{
    printf("\n==================== %s ====================\n", title);
}


/* Job details and CPU scheduling */


typedef enum { ST_NEW, ST_READY, ST_RUNNING, ST_DONE } JobStatus;

typedef struct 
{
    int id, arrival, burst, priority; /* Lower priority values mean higher priority. */
    JobStatus status;
    int remaining, start, completion, turnaround, waiting;
} Job;

typedef struct 
{
    int job_id; /* A job ID of -1 means the CPU is idle. */
    int start, end;
} Segment;

typedef struct 
{
    Segment seg[MAX_SEG];
    int nseg;
} Timeline;

typedef enum { POL_FCFS, POL_RR, POL_PRIO } Policy;
static const char *policy_name[] = {"FCFS", "Round Robin", "Priority (non-preemptive)"};

static const char *status_name(JobStatus s)
{
    switch (s) {
    case ST_NEW: return "NEW";
    case ST_READY: return "READY";
    case ST_RUNNING: return "RUNNING";
    default: return "DONE";
    }
}

/* Default jobs used when no input file is given.
 * J1 and J4 have the same priority to test the tie-breaking rule. */
static void default_jobs(Job *jobs, int *n)
{
    static const int d[][4] = {{1, 0, 5, 2}, {2, 1, 3, 1}, {3, 2, 8, 4}, {4, 3, 6, 2}, {5, 4, 2, 3}};
    *n = 5;
    for (int i = 0; i < *n; i++) {
        memset(&jobs[i], 0, sizeof jobs[i]);
        jobs[i].id = d[i][0];
        jobs[i].arrival = d[i][1];
        jobs[i].burst = d[i][2];
        jobs[i].priority = d[i][3];
        jobs[i].status = ST_NEW;
    }
}

/* Sort jobs by arrival time. If they arrive together, use the job ID. */
static void sort_jobs(Job *j, int n)
{
    for (int i = 1; i < n; i++) {
        Job key = j[i];
        int k = i - 1;
        while (k >= 0 && (j[k].arrival > key.arrival || (j[k].arrival == key.arrival && j[k].id > key.id))) {
            j[k + 1] = j[k];
            k--;
        }
        j[k + 1] = key;
    }
}

/* Check that the job details are within the allowed limits. */
static int validate_job(int id, int a, int b, int p, char *msg, size_t msz)
{
    if (id < 1 || id > 9999) { snprintf(msg, msz, "job id must be 1..9999 (got %d)", id); return -1; }
    if (a < 0 || a > 100000) { snprintf(msg, msz, "arrival must be 0..100000 (got %d)", a); return -1; }
    if (b < 1 || b > 100000) { snprintf(msg, msz, "CPU burst must be 1..100000 (got %d)", b); return -1; }
    if (p < 0 || p > 1000) { snprintf(msg, msz, "priority must be 0..1000 (got %d)", p); return -1; }
    return 0;
}

/* Read jobs from a file. Each line should contain: id arrival burst priority. */
static int load_jobs(const char *path, Job *jobs, int *n)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "error: cannot open job file '%s': %s\n", path, strerror(errno));
        return -1;
    }
    char line[256], msg[128];
    int lineno = 0;
    *n = 0;
    while (fgets(line, sizeof line, f)) {
        lineno++;
        char *p = line;
        while (isspace((unsigned char)*p)) p++;
        if (*p == '#' || *p == '\0') continue;
        int id, a, b, pr;
        char extra[2];
        if (sscanf(p, "%d %d %d %d %1s", &id, &a, &b, &pr, extra) != 4) {
            fprintf(stderr, "error: %s:%d: expected 'id arrival burst priority' (4 integers)\n", path, lineno);
            fclose(f);
            return -1;
        }
        if (validate_job(id, a, b, pr, msg, sizeof msg) != 0) {
            fprintf(stderr, "error: %s:%d: %s\n", path, lineno, msg);
            fclose(f);
            return -1;
        }
        for (int i = 0; i < *n; i++)
            if (jobs[i].id == id) {
                fprintf(stderr, "error: %s:%d: duplicate job id %d\n", path, lineno, id);
                fclose(f);
                return -1;
            }
        if (*n >= MAX_JOBS) {
            fprintf(stderr, "error: %s:%d: too many jobs (max %d)\n", path, lineno, MAX_JOBS);
            fclose(f);
            return -1;
        }
        memset(&jobs[*n], 0, sizeof jobs[*n]);
        jobs[*n].id = id; jobs[*n].arrival = a; jobs[*n].burst = b; jobs[*n].priority = pr;
        jobs[*n].status = ST_NEW;
        (*n)++;
    }
    fclose(f);
    if (*n == 0) {
        fprintf(stderr, "error: '%s' contains no jobs\n", path);
        return -1;
    }
    sort_jobs(jobs, *n);
    return 0;
}

/* Add a time slot to the timeline. Combine it with the previous slot if the same job continues. */
static void add_seg(Timeline *tl, int id, int start, int end)
{
    if (tl->nseg > 0) {
        Segment *l = &tl->seg[tl->nseg - 1];
        if (l->job_id == id && l->end == start) {
            l->end = end;
            return;
        }
    }
    if (tl->nseg < MAX_SEG) {
        tl->seg[tl->nseg].job_id = id;
        tl->seg[tl->nseg].start = start;
        tl->seg[tl->nseg].end = end;
        tl->nseg++;
    }
}

static void init_runtime(Job *j, int n)
{
    for (int i = 0; i < n; i++) {
        j[i].remaining = j[i].burst;
        j[i].start = -1;
        j[i].completion = j[i].turnaround = j[i].waiting = 0;
        j[i].status = ST_NEW;
    }
}

/* FCFS runs each job in arrival order until it finishes. */
static void run_fcfs(Job *j, int n, Timeline *tl)
{
    int t = 0;
    for (int i = 0; i < n; i++) {
        if (t < j[i].arrival) {
            add_seg(tl, -1, t, j[i].arrival);
            t = j[i].arrival;
        }
        j[i].status = ST_RUNNING;
        j[i].start = t;
        add_seg(tl, j[i].id, t, t + j[i].burst);
        t += j[i].burst;
        j[i].remaining = 0;
        j[i].completion = t;
        j[i].status = ST_DONE;
    }
}

/* Choose the highest-priority job that has arrived.
 * If priorities match, choose the earlier arrival, then the lower job ID. */
static void run_priority(Job *j, int n, Timeline *tl)
{
    int t = 0, done = 0;
    while (done < n) {
        int pick = -1, next_arr = -1;
        for (int i = 0; i < n; i++) {
            if (j[i].status == ST_DONE) continue;
            if (j[i].arrival <= t) {
                j[i].status = ST_READY;
                if (pick < 0 || j[i].priority < j[pick].priority ||
                    (j[i].priority == j[pick].priority &&
                     (j[i].arrival < j[pick].arrival ||
                      (j[i].arrival == j[pick].arrival && j[i].id < j[pick].id))))
                    pick = i;
            } else if (next_arr < 0 || j[i].arrival < next_arr) {
                next_arr = j[i].arrival;
            }
        }
        if (pick < 0) { /* Wait until the next job arrives if no job is ready. */
            add_seg(tl, -1, t, next_arr);
            t = next_arr;
            continue;
        }
        j[pick].status = ST_RUNNING;
        j[pick].start = t;
        add_seg(tl, j[pick].id, t, t + j[pick].burst);
        t += j[pick].burst;
        j[pick].remaining = 0;
        j[pick].completion = t;
        j[pick].status = ST_DONE;
        done++;
    }
}

/* In Round Robin, newly arrived jobs are added to the queue before
 * the current job is added back if it still has work left. */
static void run_rr(Job *j, int n, int quantum, Timeline *tl)
{
    int q[MAX_JOBS], head = 0, count = 0, t = 0, done = 0;
    int added[MAX_JOBS] = {0};
#define ENQ(x) do { q[(head + count) % MAX_JOBS] = (x); count++; } while (0)
    while (done < n) {
        for (int i = 0; i < n; i++)
            if (!added[i] && j[i].arrival <= t) { added[i] = 1; j[i].status = ST_READY; ENQ(i); }
        if (count == 0) { /* If the queue is empty, move time forward to the next arrival. */
            int next = -1;
            for (int i = 0; i < n; i++)
                if (!added[i] && (next < 0 || j[i].arrival < next)) next = j[i].arrival;
            add_seg(tl, -1, t, next);
            t = next;
            continue;
        }
        int cur = q[head];
        head = (head + 1) % MAX_JOBS;
        count--;
        int run = j[cur].remaining < quantum ? j[cur].remaining : quantum;
        if (j[cur].start < 0) j[cur].start = t;
        j[cur].status = ST_RUNNING;
        add_seg(tl, j[cur].id, t, t + run);
        t += run;
        j[cur].remaining -= run;
        for (int i = 0; i < n; i++)
            if (!added[i] && j[i].arrival <= t) { added[i] = 1; j[i].status = ST_READY; ENQ(i); }
        if (j[cur].remaining > 0) {
            j[cur].status = ST_READY;
            ENQ(cur);
        } else {
            j[cur].completion = t;
            j[cur].status = ST_DONE;
            done++;
        }
    }
#undef ENQ
}

typedef struct {
    double avg_ct, avg_tat, avg_wt, util;
    int makespan, dispatches;
} Metrics;

static Metrics compute_metrics(Job *j, int n, const Timeline *tl)
{
    Metrics m = {0};
    int busy = 0;
    for (int i = 0; i < n; i++) {
        j[i].turnaround = j[i].completion - j[i].arrival;
        j[i].waiting = j[i].turnaround - j[i].burst;
        m.avg_ct += j[i].completion;
        m.avg_tat += j[i].turnaround;
        m.avg_wt += j[i].waiting;
        busy += j[i].burst;
        if (j[i].completion > m.makespan) m.makespan = j[i].completion;
    }
    m.avg_ct /= n; m.avg_tat /= n; m.avg_wt /= n;
    m.util = m.makespan ? 100.0 * busy / m.makespan : 0.0;
    for (int i = 0; i < tl->nseg; i++)
        if (tl->seg[i].job_id >= 0) m.dispatches++;
    return m;
}

static void print_timeline(const Timeline *tl)
{
    printf("Execution order (timeline):\n");
    for (int base = 0; base < tl->nseg; base += 10) {
        int end = base + 10 < tl->nseg ? base + 10 : tl->nseg;
        for (int i = base; i < end; i++) {
            if (tl->seg[i].job_id < 0) printf("| IDLE  ");
            else printf("| J%-4d ", tl->seg[i].job_id);
        }
        printf("|\n");
        for (int i = base; i < end; i++) printf("%-8d", tl->seg[i].start);
        printf("%d\n", tl->seg[end - 1].end);
    }
}

static void print_table(const Job *j, int n, const Metrics *m)
{
    printf("\n%-5s %-8s %-6s %-9s %-11s %-12s %-9s %-7s\n", "Job", "Arrival", "Burst", "Priority",
           "Completion", "Turnaround", "Waiting", "Status");
    for (int i = 0; i < n; i++)
        printf("J%-4d %-8d %-6d %-9d %-11d %-12d %-9d %-7s\n", j[i].id, j[i].arrival, j[i].burst,
               j[i].priority, j[i].completion, j[i].turnaround, j[i].waiting, status_name(j[i].status));
    printf("\nAverage completion time : %.2f\n", m->avg_ct);
    printf("Average turnaround time : %.2f\n", m->avg_tat);
    printf("Average waiting time    : %.2f\n", m->avg_wt);
    printf("Makespan / CPU util.    : %d units / %.1f%%\n", m->makespan, m->util);
}

/* Run the selected scheduling algorithm using a separate copy of the jobs. */
static Metrics run_policy(Policy pol, const Job *src, int n, int quantum, Job *out, Timeline *tl)
{
    memcpy(out, src, sizeof(Job) * (size_t)n);
    tl->nseg = 0;
    init_runtime(out, n);
    if (pol == POL_FCFS) run_fcfs(out, n, tl);
    else if (pol == POL_RR) run_rr(out, n, quantum, tl);
    else run_priority(out, n, tl);
    return compute_metrics(out, n, tl);
}

static int cmd_sched(const char *which, int quantum, const char *file)
{
    Job jobs[MAX_JOBS], work[MAX_JOBS];
    Timeline tl;
    int n;
    if (file) {
        if (load_jobs(file, jobs, &n) != 0) return 1;
    } else {
        default_jobs(jobs, &n);
        sort_jobs(jobs, n);
    }
    printf("Job set (%s), %d jobs. Lower priority number = higher priority.\n",
           file ? file : "built-in default", n);
    printf("Tie-breaking: equal arrival -> lower job ID first; equal priority -> earlier arrival, then lower ID.\n");

    if (strcmp(which, "compare") == 0) {
        Metrics m[3];
        for (int p = 0; p < 3; p++) m[p] = run_policy((Policy)p, jobs, n, quantum, work, &tl);
        printf("Round Robin time quantum = %d\n\n", quantum);
        printf("%-27s %-8s %-8s %-8s %-9s %-10s\n", "Policy", "Avg CT", "Avg TAT", "Avg WT", "Makespan", "Dispatches");
        int best = 0;
        for (int p = 0; p < 3; p++) {
            printf("%-27s %-8.2f %-8.2f %-8.2f %-9d %-10d\n", policy_name[p], m[p].avg_ct, m[p].avg_tat,
                   m[p].avg_wt, m[p].makespan, m[p].dispatches);
            if (m[p].avg_wt < m[best].avg_wt) best = p;
        }
        printf("\nLowest average waiting time on this job set: %s (%.2f)\n", policy_name[best], m[best].avg_wt);
        return 0;
    }
    Policy pol;
    if (strcmp(which, "fcfs") == 0) pol = POL_FCFS;
    else if (strcmp(which, "rr") == 0) pol = POL_RR;
    else if (strcmp(which, "priority") == 0) pol = POL_PRIO;
    else {
        fprintf(stderr, "error: unknown scheduler '%s' (use fcfs, rr, priority or compare)\n", which);
        return 2;
    }
    Metrics m = run_policy(pol, jobs, n, quantum, work, &tl);
    printf("\n--- %s ---\n", policy_name[pol]);
    if (pol == POL_RR) printf("Time quantum = %d\n", quantum);
    print_timeline(&tl);
    print_table(work, n, &m);
    return 0;
}


/* Producer-consumer logger using threads and a shared queue */


typedef struct { char text[96]; } LogItem;

typedef struct {
    LogItem *buf;
    int cap, head, tail, count, max_depth;
    int producers_done;
    long full_waits, empty_waits, produced, consumed;
    pthread_mutex_t mtx;                      /* This lock protects the queue and its counters. */
    pthread_cond_t not_full, not_empty;       /* Producers wait when the queue is full; consumers wait when it is empty. */
    pthread_mutex_t out_mtx;                  /* This lock keeps console and file output from mixing together. */
    FILE *fp;
    long seq;
} LogCtx;

typedef struct { LogCtx *c; int id; int items; } ThreadArg;

static void *producer_main(void *arg)
{
    ThreadArg *a = arg;
    LogCtx *c = a->c;
    for (int k = 1; k <= a->items; k++) {
        LogItem it;
        snprintf(it.text, sizeof it.text, "producer=%d event=%d job J%d%02d submitted to lab queue", a->id, k, a->id, k);
        pthread_mutex_lock(&c->mtx);
        while (c->count == c->cap) { /* Wait here while there is no free space in the queue. */
            c->full_waits++;
            pthread_cond_wait(&c->not_full, &c->mtx);
        }
        c->buf[c->tail] = it;
        c->tail = (c->tail + 1) % c->cap;
        c->count++;
        if (c->count > c->max_depth) c->max_depth = c->count;
        c->produced++;
        pthread_cond_signal(&c->not_empty);
        pthread_mutex_unlock(&c->mtx);

        pthread_mutex_lock(&c->out_mtx);
        printf("  [producer %d] enqueued: %s\n", a->id, it.text);
        pthread_mutex_unlock(&c->out_mtx);
        usleep(1000);
    }
    return NULL;
}

static void *consumer_main(void *arg)
{
    ThreadArg *a = arg;
    LogCtx *c = a->c;
    for (;;) {
        LogItem it;
        pthread_mutex_lock(&c->mtx);
        while (c->count == 0 && !c->producers_done) { /* Wait until a message arrives or all producers have finished. */
            c->empty_waits++;
            pthread_cond_wait(&c->not_empty, &c->mtx);
        }
        if (c->count == 0 && c->producers_done) { /* Stop once the queue is empty and all producers are done. */
            pthread_mutex_unlock(&c->mtx);
            break;
        }
        it = c->buf[c->head];
        c->head = (c->head + 1) % c->cap;
        c->count--;
        c->consumed++;
        pthread_cond_signal(&c->not_full);
        pthread_mutex_unlock(&c->mtx);

        usleep(4000); /* Slow down the consumer to make the queue activity easier to observe. */
        time_t now = time(NULL);
        struct tm tmv;
        char ts[16];
        localtime_r(&now, &tmv);
        strftime(ts, sizeof ts, "%H:%M:%S", &tmv);
        pthread_mutex_lock(&c->out_mtx);
        c->seq++;
        fprintf(c->fp, "[%s] seq=%03ld consumer=%d %s\n", ts, c->seq, a->id, it.text);
        printf("  [consumer %d] logged seq=%03ld\n", a->id, c->seq);
        pthread_mutex_unlock(&c->out_mtx);
    }
    pthread_mutex_lock(&c->out_mtx);
    printf("  [consumer %d] queue drained, exiting\n", a->id);
    pthread_mutex_unlock(&c->out_mtx);
    return NULL;
}

static int cmd_logger(int np, int nc, int items, int cap)
{
    banner("Producer-Consumer Logger");
    printf("producers=%d consumers=%d events/producer=%d queue capacity=%d\n", np, nc, items, cap);
    LogCtx c;
    memset(&c, 0, sizeof c);
    c.cap = cap;
    c.buf = calloc((size_t)cap, sizeof(LogItem));
    if (!c.buf) { perror("calloc"); return 1; }
    c.fp = fopen(LOG_FILE, "w");
    if (!c.fp) { fprintf(stderr, "error: cannot create %s: %s\n", LOG_FILE, strerror(errno)); free(c.buf); return 1; }
    pthread_mutex_init(&c.mtx, NULL);
    pthread_mutex_init(&c.out_mtx, NULL);
    pthread_cond_init(&c.not_full, NULL);
    pthread_cond_init(&c.not_empty, NULL);
    fprintf(c.fp, "# CampusCompute activity log (producers=%d consumers=%d capacity=%d)\n", np, nc, cap);

    pthread_t pt[16], ct[16];
    ThreadArg pa[16], ca[16];
    int pcreated = 0, ccreated = 0, rc = 0;
    for (int i = 0; i < nc; i++) {
        ca[i] = (ThreadArg){&c, i + 1, 0};
        if (pthread_create(&ct[i], NULL, consumer_main, &ca[i]) != 0) { perror("pthread_create"); rc = 1; break; }
        ccreated++;
    }
    for (int i = 0; i < np && rc == 0; i++) {
        pa[i] = (ThreadArg){&c, i + 1, items};
        if (pthread_create(&pt[i], NULL, producer_main, &pa[i]) != 0) { perror("pthread_create"); rc = 1; break; }
        pcreated++;
    }
    for (int i = 0; i < pcreated; i++) pthread_join(pt[i], NULL);

    /* Let the consumers know that no more messages will be added. */
    pthread_mutex_lock(&c.mtx);
    c.producers_done = 1;
    pthread_cond_broadcast(&c.not_empty);
    pthread_mutex_unlock(&c.mtx);
    for (int i = 0; i < ccreated; i++) pthread_join(ct[i], NULL);

    fprintf(c.fp, "# end of log: %ld events written\n", c.consumed);
    fclose(c.fp);
    printf("\nSummary: produced=%ld consumed=%ld max queue depth=%d/%d\n", c.produced, c.consumed, c.max_depth, cap);
    printf("Synchronisation: producers blocked on FULL %ld time(s), consumers blocked on EMPTY %ld time(s)\n",
           c.full_waits, c.empty_waits);
    long expect = (long)np * items;
    if (c.produced != expect || c.consumed != expect) {
        printf("RESULT: FAIL (expected %ld events)\n", expect);
        rc = 1;
    } else {
        printf("RESULT: PASS - all %ld events logged, all %d threads joined, log file: %s\n", expect,
               pcreated + ccreated, LOG_FILE);
    }
    pthread_mutex_destroy(&c.mtx);
    pthread_mutex_destroy(&c.out_mtx);
    pthread_cond_destroy(&c.not_full);
    pthread_cond_destroy(&c.not_empty);
    free(c.buf);
    return rc;
}


/* Shared memory and named semaphores for communication between processes */


typedef struct { int id; long value; char text[40]; } Msg;
typedef struct { Msg slots[IPC_SLOTS]; int in, out; } Shared;

static int list_ipc_objects(void)
{
    DIR *d = opendir("/dev/shm");
    if (!d) { printf("  (cannot open /dev/shm: %s)\n", strerror(errno)); return -1; }
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d)))
        if (strstr(e->d_name, "campuscompute")) { printf("  /dev/shm/%s\n", e->d_name); n++; }
    closedir(d);
    if (!n) printf("  (no campuscompute IPC objects present)\n");
    return n;
}

static int sem_wait_timeout(sem_t *s, int secs)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += secs;
    int r;
    while ((r = sem_timedwait(s, &ts)) == -1 && errno == EINTR) {}
    return r;
}

/* The child process acts as the producer and opens the shared IPC objects. */
static void ipc_producer(int nmsg)
{
    int fd = shm_open(SHM_NAME, O_RDWR, 0600);
    if (fd < 0) { perror("child shm_open"); _exit(1); }
    Shared *sh = mmap(NULL, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    sem_t *mtx = sem_open(SEM_MUTEX, 0), *items = sem_open(SEM_ITEMS, 0), *slots = sem_open(SEM_SLOTS, 0);
    if (sh == MAP_FAILED || mtx == SEM_FAILED || items == SEM_FAILED || slots == SEM_FAILED) {
        perror("child open IPC");
        _exit(1);
    }
    for (int i = 1; i <= nmsg; i++) {
        if (sem_wait_timeout(slots, 5) != 0) { perror("child sem_wait(slots)"); _exit(1); }
        sem_wait(mtx);
        Msg *m = &sh->slots[sh->in];
        m->id = i;
        m->value = (long)i * i;
        snprintf(m->text, sizeof m->text, "result of job J%d", i);
        sh->in = (sh->in + 1) % IPC_SLOTS;
        sem_post(mtx);
        sem_post(items);
        printf("  [producer pid %d] wrote message %d (value=%ld)\n", getpid(), i, (long)i * i);
        fflush(stdout);
        usleep(2000);
    }
    sem_close(mtx); sem_close(items); sem_close(slots);
    munmap(sh, sizeof(Shared));
    fflush(stdout);
    _exit(0);
}

static void remove_ipc_objects(void)
{
    sem_unlink(SEM_MUTEX); sem_unlink(SEM_ITEMS); sem_unlink(SEM_SLOTS); shm_unlink(SHM_NAME);
}

static int cmd_ipc(int nmsg, int keep)
{
    banner("Shared-Memory IPC + Named Semaphores");
    printf("%d messages through a %d-slot ring buffer in POSIX shared memory.\n", nmsg, IPC_SLOTS);
    int rc = 1, fd = -1;
    Shared *sh = MAP_FAILED;
    sem_t *mtx = SEM_FAILED, *items = SEM_FAILED, *slots = SEM_FAILED;
    pid_t pid = -1;

    remove_ipc_objects(); /* Remove any leftover IPC objects from an earlier run of this program. */
    fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) { perror("shm_open"); goto out; }
    if (ftruncate(fd, sizeof(Shared)) != 0) { perror("ftruncate"); goto out; }
    sh = mmap(NULL, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (sh == MAP_FAILED) { perror("mmap"); goto out; }
    memset(sh, 0, sizeof *sh);
    mtx = sem_open(SEM_MUTEX, O_CREAT | O_EXCL, 0600, 1);
    items = sem_open(SEM_ITEMS, O_CREAT | O_EXCL, 0600, 0);
    slots = sem_open(SEM_SLOTS, O_CREAT | O_EXCL, 0600, IPC_SLOTS);
    if (mtx == SEM_FAILED || items == SEM_FAILED || slots == SEM_FAILED) { perror("sem_open"); goto out; }
    printf("Created objects:\n");
    list_ipc_objects();

    fflush(stdout);
    pid = fork();
    if (pid < 0) { perror("fork"); goto out; }
    if (pid == 0) ipc_producer(nmsg);

    long sum = 0, expect = 0;
    int got = 0, failed = 0;
    for (int i = 1; i <= nmsg; i++) expect += (long)i * i;
    for (int i = 0; i < nmsg; i++) {
        if (sem_wait_timeout(items, 5) != 0) { perror("parent sem_wait(items)"); failed = 1; break; }
        sem_wait(mtx);
        Msg m = sh->slots[sh->out];
        sh->out = (sh->out + 1) % IPC_SLOTS;
        sem_post(mtx);
        sem_post(slots);
        printf("  [consumer pid %d] read  message %d: \"%s\" value=%ld\n", getpid(), m.id, m.text, m.value);
        sum += m.value;
        got++;
    }
    if (failed) kill(pid, SIGTERM);
    int st = 0;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    pid = -1;
    printf("Producer exit status: %s %d\n", WIFEXITED(st) ? "exited with code" : "terminated by signal",
           WIFEXITED(st) ? WEXITSTATUS(st) : WTERMSIG(st));
    if (!failed && got == nmsg && sum == expect && WIFEXITED(st) && WEXITSTATUS(st) == 0) {
        printf("RESULT: PASS - received %d/%d messages, checksum %ld == %ld\n", got, nmsg, sum, expect);
        rc = 0;
    } else {
        printf("RESULT: FAIL - received %d/%d messages, checksum %ld (expected %ld)\n", got, nmsg, sum, expect);
    }
out:
    if (pid > 0) { kill(pid, SIGTERM); waitpid(pid, NULL, 0); }
    if (mtx != SEM_FAILED) sem_close(mtx);
    if (items != SEM_FAILED) sem_close(items);
    if (slots != SEM_FAILED) sem_close(slots);
    if (sh != MAP_FAILED) munmap(sh, sizeof(Shared));
    if (fd >= 0) close(fd);
    if (keep) {
        printf("--keep given: IPC objects intentionally left in place. Run './campuscompute cleanup' to remove them.\n");
        list_ipc_objects();
    } else {
        remove_ipc_objects();
        printf("Cleanup: semaphores unlinked and shared memory unlinked. Remaining objects:\n");
        list_ipc_objects();
    }
    return rc;
}


/* Banker's algorithm for checking whether a system is in a safe state */


typedef struct {
    int np, nr;
    int avail[MAXR], alloc[MAXP][MAXR], max[MAXP][MAXR];
} BState;

static void print_vec(const int *v, int n)
{
    for (int i = 0; i < n; i++) printf("%d%s", v[i], i + 1 < n ? " " : "");
}

/* Return 1 for a safe state, 0 for an unsafe state, and -1 for invalid data. */
static int banker_safety(const BState *s, int *seq)
{
    int need[MAXP][MAXR], work[MAXR], finish[MAXP] = {0};
    for (int p = 0; p < s->np; p++)
        for (int r = 0; r < s->nr; r++) {
            if (s->alloc[p][r] < 0 || s->max[p][r] < s->alloc[p][r]) {
                fprintf(stderr, "error: invalid state: P%d resource %d has Maximum < Allocation\n", p, r);
                return -1;
            }
            need[p][r] = s->max[p][r] - s->alloc[p][r]; /* Calculate the remaining resources each process may still need. */
        }
    printf("\n%-4s %-12s %-12s %-12s\n", "", "Allocation", "Maximum", "Need");
    for (int p = 0; p < s->np; p++) {
        printf("P%-3d ", p);
        for (int k = 0; k < 3; k++) {
            const int *v = k == 0 ? s->alloc[p] : k == 1 ? s->max[p] : need[p];
            char buf[32] = "";
            for (int r = 0; r < s->nr; r++) snprintf(buf + strlen(buf), sizeof buf - strlen(buf), "%d ", v[r]);
            printf("%-12s ", buf);
        }
        printf("\n");
    }
    printf("Available: ");
    print_vec(s->avail, s->nr);
    printf("\n\nSafety algorithm trace:\n");
    memcpy(work, s->avail, sizeof(int) * (size_t)s->nr);
    int cnt = 0;
    while (cnt < s->np) {
        int found = 0;
        for (int p = 0; p < s->np; p++) {
            if (finish[p]) continue;
            int ok = 1;
            for (int r = 0; r < s->nr; r++)
                if (need[p][r] > work[r]) { ok = 0; break; }
            if (!ok) continue;
            printf("  P%d: Need [", p); print_vec(need[p], s->nr);
            printf("] <= Work [");      print_vec(work, s->nr);
            for (int r = 0; r < s->nr; r++) work[r] += s->alloc[p][r];
            printf("]  -> finishes, Work becomes [");  print_vec(work, s->nr);
            printf("]\n");
            finish[p] = 1;
            seq[cnt++] = p;
            found = 1;
        }
        if (!found) break;
    }
    if (cnt == s->np) return 1;
    printf("  No remaining process has Need <= Work. Stuck processes:");
    for (int p = 0; p < s->np; p++) if (!finish[p]) printf(" P%d", p);
    printf("\n");
    return 0;
}

static int cmd_banker(const char *which)
{
    BState s;
    memset(&s, 0, sizeof s);
    if (strcmp(which, "safe") == 0) { /* Example safe state with five processes and three resource types. */
        s.np = 5; s.nr = 3;
        int av[3] = {3, 3, 2};
        int al[5][3] = {{0, 1, 0}, {2, 0, 0}, {3, 0, 2}, {2, 1, 1}, {0, 0, 2}};
        int mx[5][3] = {{7, 5, 3}, {3, 2, 2}, {9, 0, 2}, {2, 2, 2}, {4, 3, 3}};
        memcpy(s.avail, av, sizeof av);
        for (int p = 0; p < 5; p++) for (int r = 0; r < 3; r++) { s.alloc[p][r] = al[p][r]; s.max[p][r] = mx[p][r]; }
    } else if (strcmp(which, "unsafe") == 0) { /* In this example, no process can get the resources it still needs. */
        s.np = 3; s.nr = 2;
        int al[3][2] = {{1, 0}, {0, 1}, {1, 1}};
        int mx[3][2] = {{2, 1}, {1, 2}, {2, 2}};
        for (int p = 0; p < 3; p++) for (int r = 0; r < 2; r++) { s.alloc[p][r] = al[p][r]; s.max[p][r] = mx[p][r]; }
    } else {
        fprintf(stderr, "error: unknown banker scenario '%s' (use safe or unsafe)\n", which);
        return 2;
    }
    banner(strcmp(which, "safe") == 0 ? "Banker's Algorithm: SAFE case" : "Banker's Algorithm: UNSAFE case");
    int seq[MAXP];
    int r = banker_safety(&s, seq);
    if (r < 0) return 1;
    if (r) {
        printf("\nRESULT: SAFE state. Safe sequence: <");
        for (int i = 0; i < s.np; i++) printf("P%d%s", seq[i], i + 1 < s.np ? ", " : "");
        printf(">\n");
    } else {
        printf("\nRESULT: UNSAFE state - no safe sequence exists; deadlock is possible.\n");
    }
    /* Both results are expected for the test cases used here. */
    return (strcmp(which, "safe") == 0) == (r == 1) ? 0 : 1;
}


/* Create child processes and check how they finish. */


static void worker_body(int n, int mode)
{
    long s = 0;
    for (long i = 1; i <= 2000000; i++) s += i % (n + 3);
    printf("  [worker %d pid %d] computed checksum %ld\n", n, getpid(), s);
    if (mode == 1 && n == 2) {
        printf("  [worker %d pid %d] *** SIMULATED FAILURE: exiting with code 42 (not a real OS fault) ***\n", n, getpid());
        fflush(stdout);
        _exit(42);
    }
    if (mode == 1 && n == 3) {
        printf("  [worker %d pid %d] *** SIMULATED CRASH: raising SIGTERM on itself (not a real OS fault) ***\n", n, getpid());
        fflush(stdout);
        raise(SIGTERM);
        _exit(99);
    }
    fflush(stdout);
    _exit(0);
}

static int cmd_worker(const char *which)
{
    int mode;
    if (strcmp(which, "normal") == 0) mode = 0;
    else if (strcmp(which, "fail") == 0) mode = 1;
    else { fprintf(stderr, "error: unknown worker mode '%s' (use normal or fail)\n", which); return 2; }
    banner(mode ? "Workers with SIMULATED failures" : "Normal workers");
    pid_t pid[3];
    fflush(stdout);
    for (int i = 0; i < 3; i++) {
        pid[i] = fork();
        if (pid[i] < 0) { perror("fork"); return 1; }
        if (pid[i] == 0) worker_body(i + 1, mode);
        printf("Parent: started worker %d as pid %d\n", i + 1, pid[i]);
        fflush(stdout);
    }
    int ok = 0, exit_fail = 0, sig_fail = 0;
    for (int i = 0; i < 3; i++) {
        int st;
        while (waitpid(pid[i], &st, 0) < 0 && errno == EINTR) {}
        if (WIFEXITED(st) && WEXITSTATUS(st) == 0) { printf("Parent: worker %d (pid %d) completed NORMALLY (exit 0)\n", i + 1, pid[i]); ok++; }
        else if (WIFEXITED(st)) { printf("Parent: worker %d (pid %d) FAILED: exit code %d (simulated)\n", i + 1, pid[i], WEXITSTATUS(st)); exit_fail++; }
        else if (WIFSIGNALED(st)) { printf("Parent: worker %d (pid %d) FAILED: killed by signal %d (%s) (simulated)\n", i + 1, pid[i], WTERMSIG(st), strsignal(WTERMSIG(st))); sig_fail++; }
    }
    printf("Detection summary: normal=%d abnormal-exit=%d signalled=%d\n", ok, exit_fail, sig_fail);
    int match = mode == 0 ? (ok == 3) : (ok == 1 && exit_fail == 1 && sig_fail == 1);
    printf("RESULT: %s - parent reported exactly what the workers did\n", match ? "PASS" : "FAIL");
    return match ? 0 : 1;
}


/* Apply resource limits inside child processes so the parent is unaffected. */


static int wait_child(pid_t pid, int *st)
{
    while (waitpid(pid, st, 0) < 0)
        if (errno != EINTR) return -1;
    return 0;
}

static void rl_as_child(void)
{
    struct rlimit old, nw;
    getrlimit(RLIMIT_AS, &old);
    nw = old;
    nw.rlim_cur = 64UL * 1024 * 1024; /* Limit the process address space to 64 MiB. */
    if (old.rlim_max != RLIM_INFINITY && nw.rlim_cur > old.rlim_max) nw.rlim_cur = old.rlim_max;
    if (setrlimit(RLIMIT_AS, &nw) != 0) { perror("setrlimit(RLIMIT_AS)"); _exit(1); }
    printf("  [child] RLIMIT_AS soft limit set to %lu MiB\n", (unsigned long)(nw.rlim_cur >> 20));
    size_t small = 16UL << 20, big = 256UL << 20;
    char *a = malloc(small);
    printf("  [child] malloc(%zu MiB): %s\n", small >> 20, a ? "succeeded (below limit)" : "FAILED");
    if (a) { memset(a, 1, small); free(a); }
    errno = 0;
    char *b = malloc(big);
    if (b) { printf("  [child] malloc(%zu MiB) unexpectedly succeeded\n", big >> 20); free(b); fflush(stdout); _exit(1); }
    printf("  [child] malloc(%zu MiB): FAILED as expected -> %s (errno=%d)\n", big >> 20, strerror(errno), errno);
    fflush(stdout);
    _exit(errno == ENOMEM ? 0 : 1);
}

static void rl_nofile_child(void)
{
    struct rlimit rl;
    getrlimit(RLIMIT_NOFILE, &rl);
    if (rl.rlim_max != RLIM_INFINITY && rl.rlim_max < 16) _exit(1);
    rl.rlim_cur = 16;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) { perror("setrlimit(RLIMIT_NOFILE)"); _exit(1); }
    printf("  [child] RLIMIT_NOFILE soft limit set to 16 descriptors\n");
    int fds[64], n = 0;
    errno = 0;
    while (n < 64 && (fds[n] = open("/dev/null", O_RDONLY)) >= 0) n++;
    int e = errno;
    printf("  [child] opened %d extra files, then open() failed: %s (errno=%d)\n", n, strerror(e), e);
    for (int i = 0; i < n; i++) close(fds[i]);
    fflush(stdout);
    _exit(e == EMFILE ? 0 : 1);
}

static void rl_cpu_child(void)
{
    struct rlimit rl;
    getrlimit(RLIMIT_CPU, &rl);
    rl.rlim_cur = 1; /* The process receives SIGXCPU after using one CPU-second. */
    if (rl.rlim_max == RLIM_INFINITY || rl.rlim_max > 3) rl.rlim_max = 3; /* Set a hard CPU-time limit as a backup. */
    if (setrlimit(RLIMIT_CPU, &rl) != 0) { perror("setrlimit(RLIMIT_CPU)"); _exit(1); }
    printf("  [child] RLIMIT_CPU set to 1 s; entering a busy loop...\n");
    fflush(stdout);
    volatile unsigned long x = 0;
    for (;;) x++;
}

static int run_limit_test(const char *name, void (*fn)(void), int expect_sig)
{
    printf("\n-- Limit test: %s --\n", name);
    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) { fn(); _exit(1); }
    int st;
    if (wait_child(pid, &st) != 0) { perror("waitpid"); return 1; }
    int ok;
    if (expect_sig) {
        ok = WIFSIGNALED(st) && WTERMSIG(st) == SIGXCPU;
        if (WIFSIGNALED(st)) printf("Parent: child killed by signal %d (%s)\n", WTERMSIG(st), strsignal(WTERMSIG(st)));
        else printf("Parent: child exited with code %d\n", WEXITSTATUS(st));
    } else {
        ok = WIFEXITED(st) && WEXITSTATUS(st) == 0;
        printf("Parent: child exited with code %d\n", WIFEXITED(st) ? WEXITSTATUS(st) : -1);
    }
    printf("RESULT: %s - observed effect %s the expected one\n", ok ? "PASS" : "FAIL", ok ? "matches" : "does NOT match");
    return ok ? 0 : 1;
}

static int cmd_rlimit(const char *which)
{
    banner("Resource Limits (setrlimit)");
    int all = strcmp(which, "all") == 0, rc = 0, ran = 0;
    if (all || strcmp(which, "as") == 0) { rc |= run_limit_test("RLIMIT_AS (address space)", rl_as_child, 0); ran = 1; }
    if (all || strcmp(which, "nofile") == 0) { rc |= run_limit_test("RLIMIT_NOFILE (open files)", rl_nofile_child, 0); ran = 1; }
    if (all || strcmp(which, "cpu") == 0) { rc |= run_limit_test("RLIMIT_CPU (CPU seconds)", rl_cpu_child, 1); ran = 1; }
    if (!ran) { fprintf(stderr, "error: unknown limit '%s' (use as, nofile, cpu or all)\n", which); return 2; }
    printf("\nNote: limits were applied only inside child processes; the parent is unaffected.\n");
    return rc;
}


/* Remove program-created resources and check that everything was cleaned up. */


static int thread_count(void)
{
    FILE *f = fopen("/proc/self/status", "r");
    char line[128];
    int n = -1;
    if (!f) return -1;
    while (fgets(line, sizeof line, f))
        if (sscanf(line, "Threads: %d", &n) == 1) break;
    fclose(f);
    return n;
}

static void unlink_report(const char *what, int r)
{
    if (r == 0) printf("  removed   %s\n", what);
    else if (errno == ENOENT) printf("  not found %s (already clean)\n", what);
    else printf("  could not remove %s: %s\n", what, strerror(errno));
}

static int cmd_cleanup(int logs)
{
    banner("Cleanup & Verification");
    printf("Removing only objects created by CampusCompute:\n");
    unlink_report(SEM_MUTEX, sem_unlink(SEM_MUTEX));
    unlink_report(SEM_ITEMS, sem_unlink(SEM_ITEMS));
    unlink_report(SEM_SLOTS, sem_unlink(SEM_SLOTS));
    unlink_report(SHM_NAME, shm_unlink(SHM_NAME));
    if (logs) unlink_report(LOG_FILE, unlink(LOG_FILE));
    printf("\nVerification:\n");
    printf("  threads in this process : %d (1 = only the main thread)\n", thread_count());
    errno = 0;
    pid_t w = waitpid(-1, NULL, WNOHANG);
    printf("  child processes         : %s\n", (w < 0 && errno == ECHILD) ? "none remain (waitpid -> ECHILD)" : "some still exist!");
    printf("  remaining IPC objects   :\n");
    int left = list_ipc_objects();
    int ok = (left == 0) && thread_count() == 1 && w < 0;
    printf("RESULT: %s\n", ok ? "PASS - all program-created resources released" : "FAIL");
    return ok ? 0 : 1;
}


/* Handle command-line input and run the requested feature. */


static void usage(FILE *o)
{
    fprintf(o,
            "CampusCompute - job scheduling and resource management prototype\n"
            "Usage: ./campuscompute <command> [options]\n\n"
            "Commands:\n"
            "  help                          show this message\n"
            "  sched <fcfs|rr|priority|compare> [-q QUANTUM] [-f JOBFILE]\n"
            "                                CPU scheduling (default quantum %d, built-in job set)\n"
            "  logger [-p P] [-c C] [-n N] [-s SIZE]\n"
            "                                producer-consumer logger (defaults: 2 producers, 2 consumers,\n"
            "                                6 events each, queue size 4) -> writes %s\n"
            "  ipc [-n MSGS] [--keep]        shared memory + named semaphores between 2 processes\n"
            "  banker <safe|unsafe>          Banker's safety algorithm on a built-in test state\n"
            "  worker <normal|fail>          fork()/waitpid() workers; 'fail' = SIMULATED failures\n"
            "  rlimit [as|nofile|cpu|all]    setrlimit() demonstrations (default all)\n"
            "  cleanup [--logs]              remove IPC objects (and the log with --logs) made by this program\n"
            "  demo                          run every feature and print a summary\n\n"
            "Job file format: one job per line: 'id arrival burst priority' ('#' starts a comment).\n",
            DEFAULT_QUANTUM, LOG_FILE);
}

static int bad_usage(const char *msg, const char *arg)
{
    fprintf(stderr, "error: %s%s%s\n", msg, arg ? ": " : "", arg ? arg : "");
    fprintf(stderr, "Try './campuscompute help' for usage.\n");
    return 2;
}

static int cmd_demo(void)
{
    struct { const char *name; int rc; } r[10];
    int n = 0;
    banner("CampusCompute integrated demonstration");
    printf("Running all features in order: scheduling, logger, IPC, Banker, workers, rlimit, cleanup.\n");
    printf("Scheduling uses the same job set for every policy (RR quantum %d).\n", DEFAULT_QUANTUM);
    r[n].name = "FR2 CPU scheduling (FCFS/RR/Priority)";
    r[n].rc = cmd_sched("fcfs", DEFAULT_QUANTUM, NULL);
    r[n].rc |= cmd_sched("rr", DEFAULT_QUANTUM, NULL);
    r[n].rc |= cmd_sched("priority", DEFAULT_QUANTUM, NULL);
    r[n].rc |= cmd_sched("compare", DEFAULT_QUANTUM, NULL);
    n++;
    r[n].name = "FR3 Producer-consumer logger (pthreads)";
    r[n++].rc = cmd_logger(2, 2, 6, 4);
    r[n].name = "FR4 Shared memory + named semaphores";
    r[n++].rc = cmd_ipc(8, 0);
    r[n].name = "FR5 Banker's safe state";
    r[n++].rc = cmd_banker("safe");
    r[n].name = "FR5 Banker's unsafe state";
    r[n++].rc = cmd_banker("unsafe");
    r[n].name = "FR6 Normal workers (fork/waitpid)";
    r[n++].rc = cmd_worker("normal");
    r[n].name = "FR6 Simulated worker failures";
    r[n++].rc = cmd_worker("fail");
    r[n].name = "FR7 Resource limits (setrlimit)";
    r[n++].rc = cmd_rlimit("all");
    r[n].name = "FR7 Cleanup verification";
    r[n++].rc = cmd_cleanup(0);

    banner("DEMO SUMMARY");
    int bad = 0;
    for (int i = 0; i < n; i++) {
        printf("  %-45s %s\n", r[i].name, r[i].rc == 0 ? "PASS" : "FAIL");
        bad |= r[i].rc != 0;
    }
    printf("\nOverall: %s (log file %s kept for inspection; run './campuscompute cleanup --logs' to delete it)\n",
           bad ? "SOME TESTS FAILED" : "ALL TESTS PASSED", LOG_FILE);
    return bad;
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc < 2) { usage(stderr); return 2; }
    const char *cmd = argv[1];

    if (!strcmp(cmd, "help") || !strcmp(cmd, "-h") || !strcmp(cmd, "--help")) { usage(stdout); return 0; }

    if (!strcmp(cmd, "sched")) {
        if (argc < 3) return bad_usage("sched needs a policy (fcfs, rr, priority, compare)", NULL);
        const char *file = NULL;
        long q = DEFAULT_QUANTUM;
        for (int i = 3; i < argc; i++) {
            if (!strcmp(argv[i], "-q") && i + 1 < argc) {
                if (parse_long(argv[++i], 1, 1000, &q) != 0) return bad_usage("quantum must be an integer 1..1000", argv[i]);
            } else if (!strcmp(argv[i], "-f") && i + 1 < argc) {
                file = argv[++i];
            } else return bad_usage("unknown or incomplete option for sched", argv[i]);
        }
        return cmd_sched(argv[2], (int)q, file);
    }
    if (!strcmp(cmd, "logger")) {
        long p = 2, c = 2, n = 6, s = 4;
        for (int i = 2; i < argc; i++) {
            long *dst = NULL, lo = 1, hi = 16;
            if (!strcmp(argv[i], "-p")) dst = &p;
            else if (!strcmp(argv[i], "-c")) dst = &c;
            else if (!strcmp(argv[i], "-n")) { dst = &n; hi = 1000; }
            else if (!strcmp(argv[i], "-s")) { dst = &s; hi = 64; }
            if (!dst || i + 1 >= argc) return bad_usage("unknown or incomplete option for logger", argv[i]);
            if (parse_long(argv[++i], lo, hi, dst) != 0) return bad_usage("option value out of range or not a number", argv[i]);
        }
        return cmd_logger((int)p, (int)c, (int)n, (int)s);
    }
    if (!strcmp(cmd, "ipc")) {
        long n = 8;
        int keep = 0;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--keep")) keep = 1;
            else if (!strcmp(argv[i], "-n") && i + 1 < argc) {
                if (parse_long(argv[++i], 1, 1000, &n) != 0) return bad_usage("message count must be 1..1000", argv[i]);
            } else return bad_usage("unknown or incomplete option for ipc", argv[i]);
        }
        return cmd_ipc((int)n, keep);
    }
    if (!strcmp(cmd, "banker")) {
        if (argc != 3) return bad_usage("banker needs exactly one argument: safe or unsafe", NULL);
        return cmd_banker(argv[2]);
    }
    if (!strcmp(cmd, "worker")) {
        if (argc != 3) return bad_usage("worker needs exactly one argument: normal or fail", NULL);
        return cmd_worker(argv[2]);
    }
    if (!strcmp(cmd, "rlimit")) {
        if (argc > 3) return bad_usage("rlimit takes at most one argument", NULL);
        return cmd_rlimit(argc == 3 ? argv[2] : "all");
    }
    if (!strcmp(cmd, "cleanup")) {
        int logs = 0;
        if (argc == 3 && !strcmp(argv[2], "--logs")) logs = 1;
        else if (argc != 2) return bad_usage("cleanup accepts only --logs", argc > 2 ? argv[2] : NULL);
        return cmd_cleanup(logs);
    }
    if (!strcmp(cmd, "demo")) return cmd_demo();

    return bad_usage("unknown command", cmd);
}
