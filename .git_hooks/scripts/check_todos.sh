#!/bin/bash
set -e

: '
The following script shows the TODOs comments added comparing to the given/default branch
'

#############################
# UTILITIES
#############################
function exitFun {
     local exit_code=$?
     echo "[DEBUG] ${0} exited with status: ${exit_code}"
     exit ${exit_code}
}
# Call the exit function
trap exitFun EXIT

echo "[DEBUG] Executing $0 with arguments [${*}]"

#############################
# DEFAULT VALUES
#############################
FAIL_IF_TODOS=true

#############################
# READ ARGS
#############################
USAGE="Script usage:
  bash $(basename "${0}") [-w] [-b upstream/master] [-h]
Flags with * are mandatory.
  -w | --warning-mode:              If present, do not fail if TODOs found
  -b | --compare-branch <branch>:   Compare HEAD to <branch> in order to look for TODOs. If no branch provided, check local changes.
  -h | --help:                      Show script usage"

while test "${#}" -gt 0; do
  case "${1}" in
  -w | --warning-mode)
    FAIL_IF_TODOS=false
    shift 1
    ;;
  -b | --compare-branch)
    COMPARE_BRANCH=$2
    shift 2
    ;;
  -h | --help)
    echo "${USAGE}"
    exit 0
    ;;
  --)
    break
    ;;
  *)
    echo -e "[ERROR] $1 is not a recognized argument!\n${USAGE}"
    exit 1
    ;;
  esac
done

#############################
# SCRIPT BODY
#############################

# Get diff for TODOs excluding this file
if [ -n "${COMPARE_BRANCH}" ]; then
  echo "[DEBUG] Diff HEAD vs ${COMPARE_BRANCH}"
  git_todos=$(git diff --diff-filter=AM -U0 --no-prefix --pickaxe-regex -S"((//|(^|( )+\*)|#)( )*TODO)|(@todo)" "${COMPARE_BRANCH}" HEAD -- "$(basename "${0}")")
else
  echo "[DEBUG] Diff HEAD vs local changes"
  git_todos=$(git diff --diff-filter=AM -U0 --no-prefix --pickaxe-regex -S"((//|(^|( )+\*)|#)( )*TODO)|(@todo)" HEAD -- "$(basename "${0}")")
fi

# Format TODOs to print the file and the line where the TODO was found
formatted_todos=$(echo "${git_todos}" | awk '
  /^diff / {f="?"; next}
  f=="?" {if (/^\+\+\+ /) f=$0"\n"; next}
  /^@@/ {n=$3; sub(/,.*/,"",n); n=0+$3; next}
  /^\+.*(\/\/( )*TODO|(^|( )+\*)( )*TODO|#( )*TODO|@todo)/ {print f n ":" substr($0,2); f=""}
  substr($0,1,1)~/[ +]/ {n++}')

if [ -n "${git_todos}" ]; then
  echo "[DEBUG] TODOs found"
  if [ -z "${formatted_todos}" ]; then
    echo "[ERROR] TODOs found but something went wrong when formatting them"
    exit 1
  fi
  echo "${formatted_todos}"
  echo ""
  total_todos=$(grep --ignore-case --only-match --count "TODO" <<< "${formatted_todos}")
  files_with_todos=$(grep --count "+++" <<< "${formatted_todos}")
  echo "[INFO] SUMMARY -> Found ${total_todos} TODOs in ${files_with_todos} files"

  if [ "${FAIL_IF_TODOS}" = true ]; then
    echo ""
    echo "[ERROR] You have TODOs. You have to:"
    echo "  * Fix them"
    echo "  * Use git push --no-verify flag to avoid this check"
    exit 1
  fi
fi
