#!/bin/bash

# Measurement retirement
#
# As the OpenHab doesn't allow to set different retension policies for items,
# the retension process is implemented manually in the follow way:
# * OpenHab sets the tag RetDate with a retirement date as value in form YYYY-MM
# * The script executed each month and does the following:
#   - extracts points that should be retired in the current month
#   - packs them to so called append-files
#   - sends append-files to an archive server using rsync
#   - recieves a log from the archive server by a tcp connection at COMMIT_PORT
#   - deletes the retired points from the InfluxDB
#   An append-file contains retired points of a single measurement in csv format,
#   compressed by gzip.
# * The archive server runs a rsync server with a share (module), where the files
#   are copied. After ending of each rsync session, it executes the script
#   append-ohdb-retired.sh, that does the following:
#   - scans the share for append-files
#   - appends the append-files to a corresponding measurement archive file
#   - sends result to this script per tcp at COMMIT_PORT
#   - if an append-file was processed successfully, deletes it
#   The text COMMIT as result means, that all append-files was processd without
#   errors. All other texts are interpreted as error messages
# * If an append-file was sent to the archive server successfully, the retired
#   points are deleted even by errors at the archive server.
#
# See also nas:/root/append-ohdb-retired.sh

set -o pipefail

ARCHIVE_HOST="nas.srv.land-da"
RSYNC_MODULE="ohdb_retired"
COMMIT_PORT=333

# Log error
logerr() { logger --tag RETIRE -p err "$@"; }

# Log information
loginf() { logger --tag RETIRE "$@"; }

# Delete temporary files when script stops
cleanup() {
  if [ -d "$append_dir" ]; then
    rm -f "$append_dir/"*
    rm -fd "$append_dir"
  fi
}
trap "cleanup" EXIT INT QUIT TERM

# Parse csv lines and write points to append files
route_measurements() {

  awk -F ',' -v RS='\r?\n' -v APPEND_DIR="$1" '

  function is_blank(str) { return str ~ /^[[:space:]]*$/ }

  function write_file(file, str) { print str > file }

  function logerr(str) { print str | "cat >&2" }

  # Hash at the 1st position introduces annotation (4 lines) -> read it
  $0 ~ /^#/ || anno_nr == 3 {
    # Read header structure
    anno_nr++
    if (anno_nr == 1) {
      # #group,false,false,true,true,false,false,true,true,true,true
      is_bad_header=($1 != "#group")
      # A new annotation reached -> initialise variables
      delete anno
      delete header_idx
      delete annotated_files # Add new header to existing append files also
    } else if (anno_nr == 2) {
      # #datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
      is_bad_header=($1 != "#datatype")
    } else if (anno_nr == 3) {
      # #default,_result,,,,,,,,,
      is_bad_header=($1 != "#default")
    } else if (anno_nr == 4) {
      # ,result,table,_start,_stop,_time,_value,RetDate,_field,_measurement,item
      for (i = 1; i <= NF; i++) if (!is_blank($i)) header_idx[$i]=i
      is_bad_header=(is_blank(header_idx["_measurement"]) || is_blank(header_idx["_time"]))
    } else {
      # Unnown like -> raise an error
      is_bad_header=0 # Not ok
    }
    # Check the header consistency condition
    if (is_bad_header) {
      logerr("Unknown header line #"NR":")
      logerr($0)
      exit 1
    }
    # Put the current line to the annotaiton and go to the next line
    anno[anno_nr]=$0
    next
  }

  # Not a header line -> reset the header counter
  { anno_nr=0 }

  # Ignore empty lines
  is_blank($0) { next }

  # All other lines are measurements in csv format, like
  # ,,2,2023-03-12T18:39:35.002367345Z,2024-03-12T00:39:35.002367345Z,2024-03-11T22:47:59.218Z,0,2024-09,value,W_WBase_Light,W_WBase_Light

  # Consistency check: the annotation should contain 4 lines
  length(anno) != 4 {
    logerr("Annotation should have exectly 4 records, but got "length(anno)":")
    for (i=1; i <= length(anno); i++) logerr(anno[i])
    logerr("in the line:")
    logerr($0)
    exit 2
  }

  # Consistency check: Measurement should not be empty
  is_blank($header_idx["_measurement"]) {
    logerr("Measurement is empty in the line #"NR":")
    logerr($0)
    exit 3
  }

  # Consistency check: Time should not be empty
  is_blank($header_idx["_time"]) {
    logerr("Time is empty in the line #"NR":")
    logerr($0)
    exit 4
  }

  # Write point to an append file
  {
    # Prepare the append file name like append.S_UpFgl_WindDirection.2024-09.csv
    append_file=APPEND_DIR"/append."$header_idx["_measurement"]"."substr($header_idx["_time"],0,7)".csv"
    # Put annotation to new files
    if (!annotated_files[append_file]) {
      # Add an empty line before the next annotation if the file alredy exists
      if ((getline line < append_file) > 0) write_file(append_file, "")
      for (i=1; i <= length(anno); i++) write_file(append_file, anno[i])
      annotated_files[append_file]=1
    }
    # Put the current line to the append file and collect some stats
    write_file(append_file, $0)
    stats[$header_idx["_measurement"]]++
  }

  # Print stats
  END {
    if ( length(stats) == 0 ) exit 99
    for (stat in stats) {
      print stat, stats[stat]
      stat_total+=stats[stat]
    }
    print "*** Total points selected: "stat_total" ***"
  }
'
}

# Redirect all errors to logger
exec 2> >(logerr)

# Build retirement date, which is the current year and month
ret_date=$(date +%Y-%m)

loginf "Retirement for the RetDate $ret_date started"

append_dir=$(mktemp -d -t "ret_XXXXXX")
rc=$?
if [ "$rc" -ne 0 ] || [ -z "$append_dir" ]; then
  logerr "Error by creating of a temporary directory for append-files ($rc)"
  exit "$rc"
fi

loginf "... processing measurements:"

# NB: By using explizit date notation like below, influx interprets it wrongly:
# |> range(start: 2021-03-23, stop: 2025-01-01)
# It seems it takes the start year from the last 2 digits of the start date,
# i.e. from the start day 23.
# The notation -1y allows to avoid the problem
now=$(date --utc +"%Y-%m-%dT%H:%M:%SZ") # Fix cur. timestamp for delete period
influx query "
  from(bucket: \"autogen\")
    |> range(start: -5y)
    |> filter(fn: (r) => r[\"RetDate\"] == \"$ret_date\")
  " --raw | route_measurements "$append_dir" | sort 1> >(loginf)
rc=$?
if [ "$rc" -eq 99 ]; then
  loginf "No retired measurements found, processing done"
  exit 0
fi
if [ "$rc" -ne 0 ]; then
  logerr "Can't extract retained measurements from InfluxDB ($rc)"
  exit "$rc"
fi

loginf "... compressing append-files"

# Compress append files
gzip "$append_dir/"*
rc=$?
if [ "$rc" -ne 0 ]; then
  logerr "Error by compressing of append files ($rc)"
  exit "$rc"
fi

loginf "... sending append-files to archive $ARCHIVE_HOST:/$RSYNC_MODULE"

# Send append files to nas
rsync "$append_dir"/* "rsync://$ARCHIVE_HOST:/$RSYNC_MODULE/"
rc=$?
if [ "$rc" -ne 0 ]; then
  logerr "Can't copy append file to $ARCHIVE_HOST:/$RSYNC_MODULE/ ($rc)"
  exit "$rc"
fi

loginf "... waiting for a confirmation from the archive"

# Wait for a confirmation from nas
commit=$(nc -w 60 -l -s ohdb.srv.land-da -p "$COMMIT_PORT" "$ARCHIVE_HOST")
rc=$?
if [ "$commit" != "COMMIT" ]; then
  # An empty answer means timeout
  if [ -z "$commit" ]; then
    logerr "No confirmation received from $ARCHIVE_HOST ($rc)"
  else
  # Not a COMMIT answer is an error text -> forward it to logger
    logerr "An error received from $ARCHIVE_HOST:"
    logerr "$commit"
  fi
fi

loginf "... deleting retired points from InfluxDB"

# At this point it is safe to delete measurements
influx delete --bucket autogen \
  --start "1970-01-01T00:00:00Z" \
  --stop "$now" \
  --predicate "RetDate=\"$ret_date\""
rc=$?
if [ "$rc" -eq 0 ]; then
  loginf "Retired points was deleted"
else
  logerr "Can't delete retained measurements from InfluxDB ($rc)"
fi

# Done
if [ "$commit" != "COMMIT" ]
  then status=", but not confirmed by the archive server"
  else status=" successfully"
fi
loginf "Measurements was archived$status"
