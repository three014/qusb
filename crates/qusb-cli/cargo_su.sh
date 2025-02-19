#!/bin/bash
# CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E valgrind --tool=memcheck --track-origins=yes --leak-check=full --show-leak-kinds=all' cargo $@
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E' cargo $@
