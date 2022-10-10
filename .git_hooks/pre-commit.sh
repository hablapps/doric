#!/bin/sh

set -e

cd "$(git rev-parse --git-dir)/.."

#files=$(git diff --diff-filter=AM -STODO --name-only)
#echo "$files"
#if [ -n "$files" ]; then
#    if grep -Hn TODO "${files}"; then
#        echo "Blocking commit as TODO was found."
#        exit 1
#    fi
#fi

todos=$(git diff --staged --diff-filter=AM -U0 --no-prefix --pickaxe-regex -S"((//|(^|( )+\*)|#)( )*TODO)|\*( )*(@todo)" HEAD)

echo "${todos}" | awk '
  /^diff / {f="?"; next}
  f=="?" {if (/^\+\+\+ /) f=$0"\n"; next}
  /^@@/ {n=$3; sub(/,.*/,"",n); n=0+$3; next}
  /^\+.*(\/\/( )*TODO|(^|( )+\*)( )*TODO|#( )*TODO|@todo)/ {print f n ":" substr($0,2); f=""}
  substr($0,1,1)~/[ +]/ {n++}'

if [ -n "${todos}" ]; then
  echo ""
  echo "You have staged TODOs. You have to:"
  echo "  * Make sure you didn't add a TODO, staged it, removed it and forgot to stage the removal. This script only checks staged files"
  echo "  * Fix them"
  echo "  * Use --no-verify flag to avoid this check"
  exit 1
fi
