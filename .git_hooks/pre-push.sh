#!/bin/bash
set -e

FAIL_TODOS=true
CHECK_FMT=true

GO_TO="$(git rev-parse --show-toplevel)"
cd "${GO_TO}"

#############################
#        CHECK TODOS        #
#############################

if [ ${FAIL_TODOS} = true ]; then
  bash .git_hooks/scripts/check_todos.sh --compare-branch "upstream/main"
else
  bash .git_hooks/scripts/check_todos.sh --compare-branch "upstream/main" --warning-mode
fi

#############################
#       CHECK SCALAFMT      #
#############################

if [ ${CHECK_FMT} = true ]; then
  echo "[DEBUG] Checking scala format..."
  sbt --warn scalafmtCheckAll
fi

# Check & autoformat --> Disabled: I don't like not knowing the changes
#if `sbt scalafmtCheckAll`; then
#  exit 0
#else
#  sbt scalafmtAll
#  exit 1
#fi
