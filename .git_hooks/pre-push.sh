
CHECK_TODOS=true
CHECK_FMT=false

#############################
#        CHECK TODOS        #
#############################

todos=$(git diff --diff-filter=AM -U0 --no-prefix --pickaxe-regex -S"((//|(^|( )+\*)|#)( )*TODO)|(@todo)" upstream/main HEAD)

echo "${todos}" | awk '
  /^diff / {f="?"; next}
  f=="?" {if (/^\+\+\+ /) f=$0"\n"; next}
  /^@@/ {n=$3; sub(/,.*/,"",n); n=0+$3; next}
  /^\+.*(\/\/( )*TODO|(^|( )+\*)( )*TODO|#( )*TODO|@todo)/ {print f n ":" substr($0,2); f=""}
  substr($0,1,1)~/[ +]/ {n++}'

if [ ${CHECK_TODOS} = true ] && [ -n "${todos}" ]; then
  echo ""
  echo "You have TODOs. You have to:"
  echo "  * Fix them"
  echo "  * Use --no-verify flag to avoid this check"
  exit 1
fi

#############################
#       CHECK SCALAFMT      #
#############################

if [ ${CHECK_FMT} = true ]; then
  echo "sbt scalafmtCheckAll"
fi

# Check & autoformat --> Disabled: I don't like not knowing the changes
#if `sbt scalafmtCheckAll`; then
#  exit 0
#else
#  sbt scalafmtAll
#  exit 1
#fi
