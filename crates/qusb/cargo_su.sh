#!/bin/bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E perf record -e cpu_core/L1-dcache-load-misses/ -c 1000 -g --call-graph lbr --' cargo $@
