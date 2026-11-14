# CampusCompute – Intelligent Job Scheduling and Resource Management

## Overview

CampusCompute is a C-based operating-systems mini-project that demonstrates CPU
scheduling, thread synchronization, inter-process communication, deadlock
avoidance, worker processes, and process resource limits.

## Requirements

- Linux, Ubuntu, or WSL running Ubuntu
- GCC compiler
- GNU Make
- POSIX threads and POSIX shared-memory/semaphore support

On Ubuntu/WSL Ubuntu, install the build tools with:

```bash
sudo apt update
sudo apt install build-essential
```

## Files

- `campus_compute.c` – project source code
- `Makefile` – build and clean commands
- `README.md` – project instructions

## Build

Place `campus_compute.c` and `Makefile` in the same directory. From that
directory, run:

```bash
make
```

This creates the executable named `campuscompute`.

To compile without Make:

```bash
gcc -Wall -Wextra -std=c11 -O2 campus_compute.c -o campuscompute -pthread -lrt
```

To remove the executable:

```bash
make clean
```

## Run

Show the available commands:

```bash
./campuscompute help
```

### CPU scheduling

```bash
./campuscompute sched fcfs
./campuscompute sched rr -q 3
./campuscompute sched priority
./campuscompute sched compare
```

The supported scheduling algorithms are FCFS, Round Robin (RR), and Priority.
The default Round Robin quantum is 3. In Priority scheduling, a lower
priority number represents a higher priority.

An optional job input file can be supplied with `-f`:

```bash
./campuscompute sched fcfs -f jobs.txt
```

### Producer-consumer logger

```bash
./campuscompute logger
./campuscompute logger -p 2 -c 2 -n 6 -s 4
```

Options configure the number of producers (`-p`), consumers (`-c`), events per
producer (`-n`), and queue size (`-s`). Logger output is written to
`campuscompute.log`.

### Shared-memory IPC

```bash
./campuscompute ipc
./campuscompute ipc -n 8
```

Use `--keep` to retain IPC objects for demonstrating cleanup:

```bash
./campuscompute ipc -n 8 --keep
./campuscompute cleanup
```

### Banker's algorithm

```bash
./campuscompute banker safe
./campuscompute banker unsafe
```

### Worker processes

```bash
./campuscompute worker normal
./campuscompute worker fail
```

The `fail` mode deliberately simulates worker failures for demonstration; it
does not indicate an actual system failure.

### Resource limits

```bash
./campuscompute rlimit
./campuscompute rlimit as
./campuscompute rlimit nofile
./campuscompute rlimit cpu
./campuscompute rlimit all
```

### Integrated demonstration

```bash
./campuscompute demo
```

The integrated demonstration runs the main project features. It keeps the log
file so that it can be inspected afterward.

### Cleanup

Remove IPC resources:

```bash
./campuscompute cleanup
```

Remove IPC resources and the generated log:

```bash
./campuscompute cleanup --logs
```

## Preparing the Submission

1. Check that the source code, Makefile, README, and executable are present:
   ls -lh

2. Create the submission ZIP:
   zip FULL_SRN.zip campus_compute.c Makefile README.md

3. Verify the ZIP contents:
   unzip -l FULL_SRN.zip

4. Test the ZIP archive:
   unzip -t FULL_SRN.zip

## Notes

- Run commands from the directory containing the `campuscompute` executable.
- The logger creates `campuscompute.log`.
- Capture or inspect the log before running `cleanup --logs` if you need it
  for your demonstration.
- The `worker fail` command is an intentional simulated-failure test.