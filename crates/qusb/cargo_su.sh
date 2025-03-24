#!/bin/bash
# CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E' cargo $@
# CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E perf stat -d' cargo $@
# CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all --track-origins=yes' cargo $@
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E valgrind --tool=cachegrind --cache-sim=yes --branch-sim=yes' cargo $@
