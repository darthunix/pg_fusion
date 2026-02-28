#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

// macOS-only microbench for measuring raw signal delivery and ping-pong RTT.
// Build:
//   cc -O3 -std=c11 -Wall -Wextra -pedantic bench/signal_latency.c -o bench/signal_latency
// Run:
//   ./bench/signal_latency [iters] [warmup]

typedef struct Shared {
    _Atomic uint32_t ready;
    _Atomic uint32_t ack_seq;
    _Atomic uint64_t recv_ns;
} Shared;

static uint64_t now_ns(void) {
#ifdef __APPLE__
    return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

static int cmp_u64(const void *a, const void *b) {
    const uint64_t ua = *(const uint64_t *)a;
    const uint64_t ub = *(const uint64_t *)b;
    return (ua > ub) - (ua < ub);
}

static void print_stats(const char *name, uint64_t *samples, size_t n) {
    if (n == 0) {
        return;
    }

    uint64_t min = samples[0];
    uint64_t max = samples[0];
    long double sum = 0.0;
    for (size_t i = 0; i < n; i++) {
        if (samples[i] < min) {
            min = samples[i];
        }
        if (samples[i] > max) {
            max = samples[i];
        }
        sum += (long double)samples[i];
    }

    qsort(samples, n, sizeof(uint64_t), cmp_u64);
    const size_t p50_idx = (n - 1) * 50 / 100;
    const size_t p95_idx = (n - 1) * 95 / 100;
    const size_t p99_idx = (n - 1) * 99 / 100;

    printf(
        "%s: n=%zu min=%.3f us avg=%.3f us p50=%.3f us p95=%.3f us p99=%.3f us max=%.3f us\n",
        name,
        n,
        (double)min / 1000.0,
        (double)(sum / (long double)n) / 1000.0,
        (double)samples[p50_idx] / 1000.0,
        (double)samples[p95_idx] / 1000.0,
        (double)samples[p99_idx] / 1000.0,
        (double)max / 1000.0
    );
}

static void die_errno(const char *ctx) {
    fprintf(stderr, "%s: %s\n", ctx, strerror(errno));
    exit(1);
}

static Shared *map_shared(void) {
    void *ptr = mmap(NULL, sizeof(Shared), PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
    if (ptr == MAP_FAILED) {
        die_errno("mmap");
    }
    memset(ptr, 0, sizeof(Shared));
    return (Shared *)ptr;
}

static void wait_ready(const Shared *shared) {
    while (atomic_load_explicit(&shared->ready, memory_order_acquire) == 0) {
        sched_yield();
    }
}

static void run_one_way(size_t iters, size_t warmup) {
    Shared *shared = map_shared();
    const size_t total = iters + warmup;
    uint64_t *samples = calloc(iters, sizeof(uint64_t));
    if (samples == NULL) {
        die_errno("calloc one-way");
    }

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    if (sigprocmask(SIG_BLOCK, &set, NULL) != 0) {
        die_errno("sigprocmask one-way parent");
    }

    pid_t child = fork();
    if (child < 0) {
        die_errno("fork one-way");
    }
    if (child == 0) {
        int signo = 0;
        atomic_store_explicit(&shared->ready, 1, memory_order_release);
        for (size_t i = 0; i < total; i++) {
            if (sigwait(&set, &signo) != 0) {
                _exit(2);
            }
            const uint64_t recv = now_ns();
            atomic_store_explicit(&shared->recv_ns, recv, memory_order_release);
            atomic_store_explicit(&shared->ack_seq, (uint32_t)(i + 1), memory_order_release);
        }
        _exit(0);
    }

    wait_ready(shared);
    for (size_t i = 0; i < total; i++) {
        const uint64_t t0 = now_ns();
        if (kill(child, SIGUSR1) != 0) {
            die_errno("kill one-way");
        }
        const uint32_t want = (uint32_t)(i + 1);
        while (atomic_load_explicit(&shared->ack_seq, memory_order_acquire) != want) {
            ;
        }
        const uint64_t recv = atomic_load_explicit(&shared->recv_ns, memory_order_acquire);
        if (i >= warmup) {
            samples[i - warmup] = recv - t0;
        }
    }

    int status = 0;
    if (waitpid(child, &status, 0) < 0) {
        die_errno("waitpid one-way");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "one-way child failed: status=%d\n", status);
        exit(1);
    }

    print_stats("signal_one_way_sigwait", samples, iters);
    free(samples);
    munmap(shared, sizeof(*shared));
}

static void run_ping_pong(size_t iters, size_t warmup) {
    Shared *shared = map_shared();
    const size_t total = iters + warmup;
    uint64_t *samples = calloc(iters, sizeof(uint64_t));
    if (samples == NULL) {
        die_errno("calloc ping-pong");
    }

    sigset_t parent_set;
    sigemptyset(&parent_set);
    sigaddset(&parent_set, SIGUSR2);
    if (sigprocmask(SIG_BLOCK, &parent_set, NULL) != 0) {
        die_errno("sigprocmask ping-pong parent");
    }

    pid_t parent = getpid();
    pid_t child = fork();
    if (child < 0) {
        die_errno("fork ping-pong");
    }
    if (child == 0) {
        sigset_t child_set;
        sigemptyset(&child_set);
        sigaddset(&child_set, SIGUSR1);
        if (sigprocmask(SIG_BLOCK, &child_set, NULL) != 0) {
            _exit(2);
        }

        int signo = 0;
        atomic_store_explicit(&shared->ready, 1, memory_order_release);
        for (size_t i = 0; i < total; i++) {
            if (sigwait(&child_set, &signo) != 0) {
                _exit(3);
            }
            if (kill(parent, SIGUSR2) != 0) {
                _exit(4);
            }
        }
        _exit(0);
    }

    wait_ready(shared);
    int signo = 0;
    for (size_t i = 0; i < total; i++) {
        const uint64_t t0 = now_ns();
        if (kill(child, SIGUSR1) != 0) {
            die_errno("kill ping-pong");
        }
        if (sigwait(&parent_set, &signo) != 0) {
            die_errno("sigwait ping-pong parent");
        }
        const uint64_t t1 = now_ns();
        if (i >= warmup) {
            samples[i - warmup] = t1 - t0;
        }
    }

    int status = 0;
    if (waitpid(child, &status, 0) < 0) {
        die_errno("waitpid ping-pong");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "ping-pong child failed: status=%d\n", status);
        exit(1);
    }

    print_stats("signal_ping_pong_rtt", samples, iters);
    free(samples);
    munmap(shared, sizeof(*shared));
}

int main(int argc, char **argv) {
    size_t iters = 100000;
    size_t warmup = 10000;
    if (argc >= 2) {
        iters = (size_t)strtoull(argv[1], NULL, 10);
    }
    if (argc >= 3) {
        warmup = (size_t)strtoull(argv[2], NULL, 10);
    }
    if (iters == 0) {
        fprintf(stderr, "iters must be > 0\n");
        return 1;
    }

    printf("signal_latency benchmark: pid=%d iters=%zu warmup=%zu\n", getpid(), iters, warmup);
    run_one_way(iters, warmup);
    run_ping_pong(iters, warmup);
    return 0;
}
