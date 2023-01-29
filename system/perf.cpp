/* ---------------------------------------------------------------------------
 **
 ** Author: Junchang Wang @ NUPT
 **
 ** -------------------------------------------------------------------------*/

#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <string>
#include <iostream>

using namespace std;

int gen_perf_process(char *tag) {
    int perf_pid = fork();
    if (perf_pid > 0) {
            // parent
	    return perf_pid;
    } else if (perf_pid == 0) {
            // child
            perf_pid = getppid();
            char perf_pid_opt[24];
	    memset(perf_pid_opt, 0, 24);
            snprintf(perf_pid_opt, 24, "%d", perf_pid);
	    std::string events = "cache-references,cache-misses,cycles,instructions,branches,branch-misses,page-faults,cpu-migrations";
	    events += ",L1-dcache-loads,L1-dcache-load-misses,L1-icache-load-misses";
	    events += ",LLC-loads,LLC-load-misses";
	    events += ",dTLB-loads,dTLB-load-misses";
	    cerr << "=== " << tag << endl;
            char const *perfargs[12] = {"perf", "stat", "-e", events.c_str(), "-p",
                    perf_pid_opt, "--log-fd", "2", NULL};
            execvp("perf", (char **)perfargs);
            assert(0 && "perf failed to start");
    } else {
            perror("fork did not.");
    }   

    assert(0);
    return -1;
}

int kill_perf_process(int perf_pid) 
{
	int stat_val;
	pid_t child_pid;

	do {
		kill(perf_pid, SIGINT);
		child_pid = wait(&stat_val);
	} while (perf_pid != child_pid);

	return 0;
}

