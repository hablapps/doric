#!/bin/sh
set -e

GO_TO="$(git rev-parse --show-toplevel)"
cd "${GO_TO}"

#files=$(git diff --diff-filter=AM -STODO --name-only)
#echo "$files"
#if [ -n "$files" ]; then
#    if grep -Hn TODO "${files}"; then
#        echo "Blocking commit as TODO was found."
#        exit 1
#    fi
#fi

bash .git_hooks/scripts/check_todos.sh
