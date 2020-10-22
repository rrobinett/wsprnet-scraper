#!/bin/bash

# Version 0.2  Add mutex 
# Version 0.3  upload to TIMESCALE rather than keeping in local log file and azimuths at tx and rx in that order added, km only, no miles
# Version 0.4  add_azi vertex corrected, use GG suggested fields and tags, add Band as a tag and add placeholder for c2_noise from WD users with absent data for now
# Version 0.5  GG using Droplet this acount for testing screening of tx_calls against list of first two characters
# Version 0.6  GG First version to upload to a Timescale database rather than Influx
# Version 0.7  RR shorten poll loop to 30 seconds.  Don't try to truncate the daemon.log file
# Version 0.8  RR spawn a daemon to FTP clean scrape files to logs1.wsprdaemon.org
# Version 0.9  RR Optionally use ~/ftp_uploads/* as source for new scrapes rather than going to wsprnet.org
# Version 1.0  RR Optionally use API interface to get new spots from wsprnet.org and populate the TS database 'wsprnet' table 'spots'

shopt -s -o nounset          ### bash stops with error if undeclared variable is referenced

declare VERSION=1.1

export TZ=UTC LC_TIME=POSIX          ### Ensures that log dates will be in UTC

declare TS_USER=wsprnet
declare TS_PASSWORD=Ri6chaeb
declare TS_DB=wsprnet

declare UPLOAD_MODE="API"            ## Either 
declare UPLOAD_TO_WD1="no"

declare WSPRNET_SCRAPER_HOME_PATH=/home/scraper/wsprnet-scraper

#############################################
declare -i verbosity=${v:-0}         ### default to level 0, but can be overridden on the cmd line.  e.g "v=2 wsprdaemon.sh -V"

function verbosity_increment() {
    verbosity=$(( $verbosity + 1))
    echo "$(date): verbosity_increment() verbosity now = ${verbosity}"
}
function verbosity_decrement() {
    [[ ${verbosity} -gt 0 ]] && verbosity=$(( $verbosity - 1))
    echo "$(date): verbosity_decrement() verbosity now = ${verbosity}"
}

function setup_verbosity_traps() {
    trap verbosity_increment SIGUSR1
    trap verbosity_decrement SIGUSR2
}

function signal_verbosity() {
    local up_down=$1
    local pid_files=$(shopt -s nullglob ; echo ${WSPRNET_SCRAPER_HOME_PATH}/*.pid)

    if [[ -z "${pid_files}" ]]; then
        echo "No *.pid files in ${WSPRNET_SCRAPER_HOME_PATH}"
        return
    fi
    local file
    for file in ${pid_files} ; do
        local debug_pid=$(cat ${file})
        if ! ps ${debug_pid} > /dev/null ; then
            echo "PID ${debug_pid} from ${file} is not running"
        else
            echo "Signaling verbosity change to PID ${debug_pid} from ${file}"
            kill -SIGUSR${up_down} ${debug_pid}
        fi
    done
}

### executed by cmd line '-d'
function increment_verbosity() {
    signal_verbosity 1
}
### executed by cmd line '-D'
function decrement_verbosity() {
    signal_verbosity 2
}

######################### Uploading to WD1 section ############################
declare UPLOAD_QUEUE_DIR=${WSPRNET_SCRAPER_HOME_PATH}/upload.d    ### On the WD server which is scraping wsprnet.org, this is where it puts parsed scrape files for upload to WD1

function upload_to_wd1_daemon() {
    local upload_user=${SIGNAL_LEVEL_FTP_LOGIN-uploader}
    local upload_password=${SIGNAL_LEVEL_FTP_PASSWORD-xahFie6g}

    mkdir -p ${UPLOAD_QUEUE_DIR}
    cd ${UPLOAD_QUEUE_DIR}
    shopt -s nullglob
    while true; do
        [[ $verbosity -ge 2 ]] && echo "$(date): upload_to_wd1_daemon() looking for files to upload"
        local file_list=()
        while file_list=( * ) && [[ ${#file_list[@]} -gt 0 ]]; do
            [[ $verbosity -ge 2 ]] && echo "$(date): upload_to_wd1_daemon() found files '${file_list[@]}' to upload"
            local file
            for file in ${file_list[@]}; do
                local upload_url=${SIGNAL_LEVEL_FTP_URL-logs1.wsprdaemon.org}/${file}
                [[ $verbosity -ge 2 ]] && echo "$(date): upload_to_wd1_daemon() uploading file '${file}'"
                curl -s -m 30 -T ${file}  --user ${upload_user}:${upload_password} ftp://${upload_url}
                local ret_code=$?
                if [[ ${ret_code} -eq 0 ]]; then
                    [[ $verbosity -ge 1 ]] && echo "$(date): upload_to_wd1_daemon() upload of file '${file}' was successful"
                    rm ${file}
                else
                    [[ $verbosity -ge 1 ]] && echo "$(date): upload_to_wd1_daemon() upload of file '${file}' failed.  curl => ${ret_code}"
                fi
            done
        done
        sleep 10
    done
}

function queue_upload_to_wd1() {
    local scrapes_to_add_file=$1

    mkdir -p ${UPLOAD_QUEUE_DIR}
    local epoch=$(date +%s)
    local upload_file_name="${scrapes_to_add_file%_*}_${epoch}.txt"
    while [[ -f ${upload_file_name} ]]; do
        [[ $verbosity -ge 1 ]] && echo "$(date): queue_upload_to_wd1() queued file '${UPLOAD_QUEUE_DIR}/${upload_file_name}' exists, Sleep 1 second and try again"
        sleep 1
        epoch=$(date +%s)
        upload_file_name="${scrapes_to_add_file%_*}_${epoch}.txt"
    done
    cp -p ${scrapes_to_add_file} ${UPLOAD_QUEUE_DIR}/${upload_file_name}
    bzip2 ${UPLOAD_QUEUE_DIR}/${upload_file_name}
    [[ $verbosity -ge 2 ]] && echo "$(date): queue_upload_to_wd1() queued ${scrapes_to_add_file} bzipped as ${UPLOAD_QUEUE_DIR}/${upload_file_name}.bz2"
}

################### API scrape section ##########################################################

declare UPLOAD_WN_BATCH_PYTHON_CMD=${WSPRNET_SCRAPER_HOME_PATH}/ts_upload_batch.py
declare UPLOAD_SPOT_SQL='INSERT INTO spots (wd_time, "Spotnum", "Date", "Reporter", "ReporterGrid", "dB", "MHz", "CallSign", "Grid", "Power", "Drift", distance, azimuth, "Band", version, code, 
    wd_band, wd_c2_noise, wd_rms_noise, wd_rx_az, wd_rx_lat, wd_rx_lon, wd_tx_az, wd_tx_lat, wd_tx_lon, wd_v_lat, wd_v_lon ) 
    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );'

declare UPLOAD_WN_BATCH_TS_CONNECT_INFO="dbname='${TS_DB}' user='${TS_USER}' host='localhost' password='${TS_PASSWORD}'"

### Executes a batch upload of csv file $2 using SQL in $1.  TS login info is $3
function create_wn_spots_batch_upload_python() {
    cat > ${UPLOAD_WN_BATCH_PYTHON_CMD} <<EOF
# -*- coding: utf-8 -*-
#!/usr/bin/python
# March-May  2020  Gwyn Griffiths
# ts_batch_upload.py   a program to read in a spots file scraped from wsprnet.org by scraper.sh and upload to a TimescaleDB
# Version 1.2 May 2020 batch upload from a parsed file. Takes about 1.7s compared with 124s for line by line
# that has been pre-formatted with an awk line to be in the right order and have single quotes around the time and character fields
# Added additional diagnostics to identify which part of the upload fails (12 in 1936 times)
import psycopg2                  # This is the main connection tool, believed to be written in C
import psycopg2.extras           # This is needed for the batch upload functionality
import csv                       # To import the csv file
import sys                       # to get at command line argument with argv

# initially set the connection flag to be None
conn=None
connected="Not connected"
cursor="No cursor"
execute="Not executed"
commit="Not committed"
ret_code=0

batch_file_path=sys.argv[1]
sql=sys.argv[2]
connect_info=sys.argv[3]

try:
    with open (batch_file_path) as csv_file:
        csv_data = csv.reader(csv_file, delimiter=',')
        try:
               # connect to the PostgreSQL database
               #print ("Trying to  connect")
               conn = psycopg2.connect( connect_info )
               connected="Connected"
               #print ("Appear to have connected")
               # create a new cursor
               cur = conn.cursor()
               cursor="Got cursor"
               # execute the INSERT statement
               psycopg2.extras.execute_batch(cur,sql,csv_data)
               execute="Executed"
               #print ("After the execute")
               # commit the changes to the database
               conn.commit()
               commit="Committed"
               # close communication with the database
               cur.close()
               #print (connected,cursor, execute,commit)
        except:
               print ("Unable to record spot file do the database:",connected,cursor, execute,commit)
               ret_code=1
finally:
        if conn is not None:
            conn.close()
        sys.exit(ret_code)
EOF
}

function wn_spots_batch_upload() {
    local csv_file=$1

    [[ $verbosity -ge 2 ]] && echo "$(date): wn_spots_batch_upload() record ${csv_file} to TS"
    if [[ ! -f ${UPLOAD_WN_BATCH_PYTHON_CMD} ]]; then
        create_wn_spots_batch_upload_python
    fi
    python3 ${UPLOAD_WN_BATCH_PYTHON_CMD} ${csv_file} "${UPLOAD_SPOT_SQL}" "${UPLOAD_WN_BATCH_TS_CONNECT_INFO}"
    local ret_code=$?
    if [[ ${ret_code} -ne 0 ]]; then
        [[ $verbosity -ge 1 ]] && echo "$(date): wn_spots_batch_upload() UPLOAD_WN_BATCH_PYTHON_CMD => ${ret_code}"
    fi
    [[ $verbosity -ge 2 ]] && echo "$(date): wn_spots_batch_upload() record ${csv_file} => ${ret_code}"
    return ${ret_code}
}


declare WSPRNET_SESSION_ID_FILE=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet_session_info.html

function wpsrnet_login() {
    [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_login() executing curl to login"
    timeout 60 curl -s -d '{"name":"ai6vn", "pass":"2nsSm2c2UvmRbr4x"}' -H "Content-Type: application/json" -X POST http://wsprnet.org/drupal/rest/user/login > ${WSPRNET_SESSION_ID_FILE}
    local ret_code=$?
    if [[ ${ret_code} -eq 0 ]]; then
        local sessid=$(cat ${WSPRNET_SESSION_ID_FILE} | tr , '\n' | sed -n '/sessid/s/^.*://p' | sed 's/"//g')
        local session_name=$(cat ${WSPRNET_SESSION_ID_FILE} | tr , '\n' | sed -n '/session_name/s/^.*://p' | sed 's/"//g')
        if [[ -z "${sessid}" ]] || [[ -z "${session_name}" ]]; then
            [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_login()  failed to extract sessid=${sessid} and/or session_name${session_name}"
            rm -f ${WSPRNET_SESSION_ID_FILE}
            ret_code=2
        else
            [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_login() login was successful"
        fi
    else
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_login()  curl login failed => ${ret_code}"
        rm -f ${WSPRNET_SESSION_ID_FILE}
   fi
    return ${ret_code}
}

declare WSPRNET_HTML_SPOT_FILE=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet_spots.html
declare WSPRNET_LAST_SPOTNUM=0

function wpsrnet_get_spots() {
    [[ ${verbosity} -ge 2 ]] && echo "$(date): wpsrnet_get_spots() starting"
    if [[ ! -f ${WSPRNET_SESSION_ID_FILE} ]] || [[ ! -s ${WSPRNET_SESSION_ID_FILE} ]]; then
       if ! wpsrnet_login ; then
           [[ ${verbosity} -ge 2 ]] && echo "$(date): wpsrnet_get_spots() failed to login"
           return 1
       fi
    fi
    local sessid=$(cat ${WSPRNET_SESSION_ID_FILE} | tr , '\n' | sed -n '/sessid/s/^.*://p' | sed 's/"//g')
    local session_name=$(cat ${WSPRNET_SESSION_ID_FILE} | tr , '\n' | sed -n '/session_name/s/^.*://p' | sed 's/"//g')
    if [[ -z "${sessid}" ]] || [[ -z "${session_name}" ]]; then
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots(): wpsrnet_login() failed to extract sessid=${sessid} and/or session_name${session_name}"
        rm -f ${WSPRNET_SESSION_ID_FILE}
        ret_code=2
    fi
    local session_token="${session_name}=${sessid}"
    [[ ${verbosity} -ge 2 ]] && echo "$(date): wpsrnet_get_spots(): got wsprnet session_token = ${session_token}"
 
    if [[ ${WSPRNET_LAST_SPOTNUM} -eq 0 ]]; then
        ### Get the largest Spotnum from the TS DB
        ### I need to redirect the output to a file or the psql return code gets lost
        local psql_output_file=./psql.out
        PGPASSWORD=${TS_PASSWORD}  psql -t -U ${TS_USER} -d ${TS_DB}  -c 'select "Spotnum" from spots order by "Spotnum" desc limit 1 ;' > ${psql_output_file}
        local ret_code=$?
        if [[ ${ret_code} -ne 0 ]]; then
            [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots(): psql( ${TS_USER}/${TS_PASSWORD}/${TS_DB}) for latest TS returned error => ${ret_code}"
            exit 1
        fi
        local psql_output=$(cat ${psql_output_file})
        local last_spotnum=$(tr -d ' ' <<< "${psql_output}")
        if [[ -z "${last_spotnum}" ]] || [[ ${last_spotnum} -eq 0 ]]; then
            [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots(): at startup failed to get a Spotnum from TS"
            exit 1
        fi
        WSPRNET_LAST_SPOTNUM=${last_spotnum}
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots(): at startup using highest Spotnum ${last_spotnum} from TS, not 0"
    fi
    [[ ${verbosity} -ge 2 ]] && echo "$(date): wpsrnet_get_spots() starting curl download for spotnum_start=${WSPRNET_LAST_SPOTNUM}"
    local start_seconds=${SECONDS}
    local curl_str="'{spotnum_start:\"${WSPRNET_LAST_SPOTNUM}\",band:\"All\",callsign:\"\",reporter:\"\",exclude_special:\"1\"}'"
    curl -s -m ${WSPRNET_CURL_TIMEOUT-10} -b "${session_token}" -H "Content-Type: application/json" -X POST -d ${curl_str}  "http://wsprnet.org/drupal/wsprnet/spots/json?band=All&spotnum_start=${WSPRNET_LAST_SPOTNUM}&exclude_special=0" > ${WSPRNET_HTML_SPOT_FILE}
    local ret_code=$?
    local end_seconds=${SECONDS}
    local curl_seconds=$(( end_seconds - start_seconds))
    if [[ ${ret_code} -ne 0 ]]; then
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots() curl download failed => ${ret_code} after ${curl_seconds} seconds"
    else
        if grep -q "You are not authorized to access this page." ${WSPRNET_HTML_SPOT_FILE}; then
            [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots() wsprnet.org login failed"
            rm ${WSPRNET_SESSION_ID_FILE}
            ret_code=1
        else
            if ! grep -q "Spotnum" ${WSPRNET_HTML_SPOT_FILE} ; then
                [[ ${verbosity} -ge 1 ]] && echo "$(date): wpsrnet_get_spots() WARNING: ${WSPRNET_HTML_SPOT_FILE} contains no spots"
                ret_code=2
            else
                local download_size=$( cat ${WSPRNET_HTML_SPOT_FILE} | wc -c)
                [[ ${verbosity} -ge 2 ]] && echo "$(date): wpsrnet_get_spots() curl downloaded ${download_size} bytes of spot info after ${curl_seconds} seconds"
            fi
        fi
    fi
    return ${ret_code}
}

### Convert the html we get from wsprnet to a csv file
### The html records are in the order Spotnum,Date,Reporter,ReporterGrid,dB,Mhz,CallSign,Grid,Power,Drift,distance,azimuth,Band,version,code
### The html records are in the order  1       2     3         4         5  6     7       8     9    10      11      12     13     14    15
function wsprnet_to_csv() {
    local wsprnet_html_spot_file=$1
    local wsprnet_csv_spot_file=$2
    
    local lines=$(cat ${wsprnet_html_spot_file} | sed 's/[{]/\n/g; s/[}],//g; s/"//g; s/[}]/\n/' | sed '/^\[/d; /^\]/d; s/[a-zA-Z]*://g')
          lines="${lines//\\/}"          ### Strips the '\' out of the call sign and reporter fields, e.g. 'N6GN\/P' becomes 'N6GN/P''
    local sorted_lines=$(sort <<< "${lines}")      ### Now sorted by spot id (which ought to be by time, too)
    local sorted_lines_array=()
    mapfile -t sorted_lines_array <<< "${sorted_lines}" 

    local html_spotnum_count=$(grep -o Spotnum ${wsprnet_html_spot_file} | wc -l)
    if [[ ${html_spotnum_count} -ne ${#sorted_lines_array[@]} ]]; then
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wsprnet_to_csv() WARNING: found ${html_spotnum_count} spotnums in the html file, but only ${#sorted_lines_array[@]} in our plaintext version of it"
    fi

    [[ ${verbosity} -ge 2 ]] && echo "$(date): wsprnet_to_csv() found ${#sorted_lines_array[@]} elements in sorted_lines_array[@]"

    local sorted_lines_array_count=${#sorted_lines_array[@]}
    local max_index=$((${sorted_lines_array_count} - 1))
    local first_line=${sorted_lines_array[0]}
    local last_line=${sorted_lines_array[${max_index}]}
    [[ ${verbosity} -ge 2 ]] && echo "$(date): wsprnet_to_csv() extracted ${sorted_lines_array_count} lines (max index = ${max_index}) from the html file.  After sort first= ${first_line}, last= ${last_line}"

    ### To monitor and validate the spots, check for gaps in the sequence numbers
    local total_gaps=0
    local total_missing=0
    local max_gap_size=0
    local expected_seq=0
    for index in $(seq 0 ${max_index}); do
        local got_seq=${sorted_lines_array[${index}]//,*}
        local next_seq=$(( ${got_seq} + 1 ))
        if [[ ${index} -eq 0 ]]; then
            expected_seq=${next_seq}
        else
            local gap_size=$(( got_seq - expected_seq ))
            if [[ ${gap_size} -ne 0  ]]; then
               total_gaps=$(( total_gaps + 1 ))
               total_missing=$(( total_missing + gap_size ))
               if [[ ${gap_size} -gt ${max_gap_size} ]]; then
                   max_gap_size=${gap_size}
               fi
               [[ ${verbosity} -ge 1 ]] && printf "$(date): wsprnet_to_csv() found gap of %3d at index %4d:  Expected ${expected_seq}, got ${got_seq}\n" "${gap_size}" "${index}"
           fi
           expected_seq=${next_seq}
       fi
    done
    if [[ ${verbosity} -ge 1 ]] && [[ ${max_gap_size} -gt 0 ]] && [[ ${WSPRNET_LAST_SPOTNUM} -ne 0 ]]; then
        printf "$(date): wsprnet_to_csv() found ${total_gaps} gaps missing a total of ${total_missing} spots. The max gap was of ${max_gap_size} spot numbers\n"
    fi

    unset lines   ### just to be sure we don't use it again

    ### Prepend TS format times derived from the epoch times in field #2 to each spot line in the sorted 
    ### There are probably only 1 or 2 different dates for the spot lines.  So use awk or sed to batch convert rather than examining each line.
    local dates=( $(awk -F , '{print $2}' <<< "${sorted_lines}" | sort -u) )
    ### Prepend the TS format date to each of the API lines
    local api_lines=""
    rm -f ${wsprnet_csv_spot_file}
    for date in "${dates[@]}"; do
        local ts_date=$(date -d @${date} +%Y-%m-%d:%H:%M)
        api_lines="${api_lines}$(awk "/${date}/{print \"${ts_date},\" \$0}" <<< "${sorted_lines}" )"
        awk "/${date}/{print \"${ts_date},\" \$0}" <<< "${sorted_lines}"  >> ${wsprnet_csv_spot_file}
    done

    local csv_spotnum_count=$( wc -l < ${wsprnet_csv_spot_file})
    if [[ ${csv_spotnum_count} -ne ${#sorted_lines_array[@]} ]]; then
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wsprnet_to_csv() WARNING: found ${#sorted_lines_array[@]} in our plaintext of the html file, but only ${csv_spotnum_count} is the csv version of it"
    fi

    local first_spot_array=(${sorted_lines_array[0]//,/ })
    local last_spot_array=(${sorted_lines_array[${max_index}]//,/ })
    [[ ${verbosity} -ge 1 ]] && printf "$(date): wsprnet_to_csv() got scrape with %4d spots from %4d wspr cycles. First spot: ${first_spot_array[0]}/${first_spot_array[1]}, Last spot: ${last_spot_array[0]}/${last_spot_array[1]}\n" "${#dates[@]}" "${#sorted_lines_array[@]}"

    ### For monitoring and validation, document the gap between the last spot of the last scrape and the first spot of this scrape
    local spot_num_gap=$(( ${first_spot_array[0]} - ${WSPRNET_LAST_SPOTNUM} ))
    if [[ ${WSPRNET_LAST_SPOTNUM} -ne 0 ]] && [[ ${spot_num_gap} -gt 2 ]]; then
        [[ ${verbosity} -ge 1 ]] && printf "$(date): wsprnet_to_csv() found gap of %4d spotnums between last spot #${WSPRNET_LAST_SPOTNUM} and first spot #${first_spot_array[0]} of this scrape\n" "${spot_num_gap}"
    fi
    ### Remember the current last spot for the next call to this function
    WSPRNET_LAST_SPOTNUM=${last_spot_array[0]}
}

declare WSPRNET_OFFSET_FIRST_SEC=55
declare WSPRNET_OFFSET_GAP=30
declare WSPRNET_OFFSET_SECS=""
offset=${WSPRNET_OFFSET_FIRST_SEC}
while [[ ${offset} -lt 120 ]]; do
   WSPRNET_OFFSET_SECS="${WSPRNET_OFFSET_SECS} ${offset}"
   offset=$(( offset + WSPRNET_OFFSET_GAP ))
done

function api_wait_until_next_offset() {
    local epoch_secs=$(date +%s)
    local cycle_offset=$(( ${epoch_secs} % 120 ))

    [[ ${verbosity} -ge 3 ]] && echo "$(date): api_wait_until_next_offset() starting at offset ${cycle_offset}"
    for secs in ${WSPRNET_OFFSET_SECS}; do
        secs_to_next=$(( ${secs} - ${cycle_offset} ))    
        [[ ${verbosity} -ge 3 ]] && echo "$(date): api_wait_until_next_offset() ${secs} - ${cycle_offset} = ${secs_to_next} secs_to_next"
        if [[ ${secs_to_next} -le 0 ]]; then
            [[ ${verbosity} -ge 3 ]] && echo "$(date): api_wait_until_next_offset() offset secs ${cycle_offset} is greater than test offset ${secs}"
        else
            [[ ${verbosity} -ge 3 ]] && echo "$(date): api_wait_until_next_offset() found ${secs}"
            break
        fi
    done
    local secs_to_next=$(( secs - cycle_offset ))
    if [[ ${secs_to_next} -le 0 ]]; then
       ### we started after 110 seconds
       secs=${WSPRNET_OFFSET_FIRST_SEC}
       secs_to_next=$(( 120 - cycle_offset + secs ))
    fi
    [[ ${verbosity} -ge 2 ]] && echo "$(date): api_wait_until_next_offset() starting at offset ${cycle_offset}, next offset ${secs}, so secs_to_wait = ${secs_to_next}"
    sleep ${secs_to_next}
}

# G3ZIL add tx and rx lat, lon and azimuths and path vertex using python script. In the main program, call this function with a file path/name for the input file
# the appended data gets stored into this file which can be examined. Overwritten each acquisition cycle.
declare WSPRNET_CSV_SPOT_FILE=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet_spots.csv              ### This csv is derived from the html returned by the API and has fields 'wd_date, spotnum, epoch, ...' sorted by spotnum
declare WSPRNET_CSV_SPOT_AZI_FILE=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet_spots_azi.csv      ### This csv is derived from WSPRNET_CSV_SPOT_FILE and includes wd_XXXX fields calculated by azi_calc.py and added to each spot line
declare AZI_PYTHON_CMD=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet_azi_calc.py

### Takes a spot file created by API and adds azimuth fields to it
function wsprnet_add_azi() {
    local api_spot_file=$1
    local api_azi_file=$2

    [[ ${verbosity} -ge 2 ]] && echo "$(date): wsprnet_add_azi() process ${api_spot_file} to create ${api_azi_file}"

    if [[ ! -f ${AZI_PYTHON_CMD} ]]; then
        wsprnet_create_azi_python
    fi
    python3 ${AZI_PYTHON_CMD} ${api_spot_file} ${api_azi_file}
    local ret_code=$?
    if [[ ${ret_code} -ne 0 ]]; then
        [[ ${verbosity} -ge 1 ]] && echo "$(date): wsprnet_add_azi() python3 ${AZI_PYTHON_CMD} ${api_spot_file} ${api_azi_file} => ${ret_code}"
    else
        [[ ${verbosity} -ge 2 ]] && echo "$(date): wsprnet_add_azi() python3 ${AZI_PYTHON_CMD} ${api_spot_file} ${api_azi_file} => ${ret_code}"
    fi
    return ${ret_code}
}

#G3ZIL python script that gets copied into /tmp/azi_calc.py and is run there
function wsprnet_create_azi_python() {
    [[ ${verbosity} -ge 2 ]] && echo "$(date): create_azi_python() creating ${AZI_PYTHON_CMD}"

    cat > ${AZI_PYTHON_CMD} <<EOF
# -*- coding: utf-8 -*-
# Filename: wsprnet_azi_calc.py
# February  2020  Gwyn Griffiths

# Take a scraper latest_log.txt file, extract the receiver and transmitter Maidenhead locators and calculates azimuth at tx and rx in that order
# Needs one argument, file path for latest_log.txt
# V1.0 outputs the azi-appended data to a file spots+azi.csv that is overwritten next 3 min cycle, this file appended to the master
# V1.1 also outputs lat and lo for tx and rx and the vertex, the point on the path nearest the pole (highest latitude) for the short path
# The vertex in the other hemisphere has the sign of the latitude reversed and 180˚ added to the longitude
# V1.2 The operating band is derived from the frequency, 60 and 60eu and 80 and 80eu are reported as 60 and 80
# Miles are not copied to the azi-appended file
# In the script the following lines preceed this code and there's an EOF added at the end
# V1.3 RR modified to accept API spot lines

import numpy as np
from numpy import genfromtxt
import sys
import csv

# define function to convert 4 or 6 character Maidenhead locator to lat and lon in degrees
def loc_to_lat_lon (locator):
    locator=locator.strip()
    decomp=list(locator)
    lat=(((ord(decomp[1])-65)*10)+(ord(decomp[3])-48)+(1/2)-90)
    lon=(((ord(decomp[0])-65)*20)+((ord(decomp[2])-48)*2)+(1)-180)
    if len(locator)==6:
        if (ord(decomp[4])) >88:    # check for case of the third pair, likely to  be lower case
            ascii_base=96
        else:
            ascii_base=64
        lat=lat-(1/2)+((ord(decomp[5])-ascii_base)/24)-(1/48)
        lon=lon-(1)+((ord(decomp[4])-ascii_base)/12)-(1/24)
    return(lat, lon)

# get the path to the latest_log.txt file from the command line
spots_file_path=sys.argv[1]
azi_file_path=sys.argv[2]

# now read in lines file, as a single string, skip over lines with unexpected number of columns
spot_lines=genfromtxt(spots_file_path, dtype='str', delimiter=',', loose=True, invalid_raise=False)
# get number of lines
n_lines=len(spot_lines)
# split out the rx and tx locators
tx_locators=list(spot_lines[:,8])
rx_locators=list(spot_lines[:,4])

# open file for output as a csv file, to which we will copy original data and the tx and rx azimuths
with open(azi_file_path, "w") as out_file:
    out_writer=csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # loop to calculate  azimuths at tx and rx (wsprnet only does the tx azimuth)
    for i in range (0 , n_lines):
        (tx_lat,tx_lon)=loc_to_lat_lon (tx_locators[i])    # call function to do conversion, then convert to radians
        phi_tx_lat = np.radians(tx_lat)
        lambda_tx_lon = np.radians(tx_lon)
        (rx_lat,rx_lon)=loc_to_lat_lon (rx_locators[i])    # call function to do conversion, then convert to radians
        phi_rx_lat = np.radians(rx_lat)
        lambda_rx_lon = np.radians(rx_lon)
        delta_phi = (phi_tx_lat - phi_rx_lat)
        delta_lambda=(lambda_tx_lon-lambda_rx_lon)

        # calculate azimuth at the rx
        y = np.sin(delta_lambda) * np.cos(phi_tx_lat)
        x = np.cos(phi_rx_lat)*np.sin(phi_tx_lat) - np.sin(phi_rx_lat)*np.cos(phi_tx_lat)*np.cos(delta_lambda)
        rx_azi = (np.degrees(np.arctan2(y, x))) % 360

        # calculate azimuth at the tx
        p = np.sin(-delta_lambda) * np.cos(phi_rx_lat)
        q = np.cos(phi_tx_lat)*np.sin(phi_rx_lat) - np.sin(phi_tx_lat)*np.cos(phi_rx_lat)*np.cos(-delta_lambda)
        tx_azi = (np.degrees(np.arctan2(p, q))) % 360
            
        # calculate the vertex, the lat lon at the point on the great circle path nearest the nearest pole, this is the highest latitude on the path
        # no need to calculate special case of both transmitter and receiver on the equator, is handled OK
        # Need special case for any meridian, where vertex longitude is the meridian longitude and the vertex latitude is the lat nearest the N or S pole
        if tx_lon==rx_lon:
            v_lon=tx_lon
            v_lat=max([tx_lat, rx_lat], key=abs)
        else:
            v_lat=np.degrees(np.arccos(np.sin(np.radians(rx_azi))*np.cos(phi_rx_lat)))
        if v_lat>90.0:
            v_lat=180-v_lat
        if rx_azi<180:
            v_lon=((rx_lon+np.degrees(np.arccos(np.tan(phi_rx_lat)/np.tan(np.radians(v_lat)))))+360) % 360
        else:
            v_lon=((rx_lon-np.degrees(np.arccos(np.tan(phi_rx_lat)/np.tan(np.radians(v_lat)))))+360) % 360
        if v_lon>180:
            v_lon=-(360-v_lon)
        # now test if vertex is not  on great circle track, if so, lat/lon nearest pole is used
        if v_lon < min(tx_lon, rx_lon) or v_lon > max(tx_lon, rx_lon):
        # this is the off track case
            v_lat=max([tx_lat, rx_lat], key=abs)
            if v_lat==tx_lat:
                v_lon=tx_lon
            else:
                v_lon=rx_lon
        # derive the band in metres (except 70cm and 23cm reported as 70 and 23) from the frequency
        freq=int(10*float(spot_lines[i,6]))
        band=9999
        if freq==1:
            band=2200
        if freq==4:
            band=630
        if freq==18:
            band=160
        if freq==35:
            band=80
        if freq==52 or freq==53:
            band=60
        if freq==70:
            band=40
        if freq==101:
            band=30
        if freq==140:
            band=20
        if freq==181:
            band=17
        if freq==210:
            band=15
        if freq==249:
            band=12
        if freq==281:
            band=10
        if freq==502:
            band=6
        if freq==503:
            band=6
        if freq==701:
            band=4
        if freq==1442:
            band=2
        if freq==1444:
            band=2
        if freq==4323:
            band=70
        if freq==12965:
            band=23
        # output the original data and add lat lon at tx and rx, azi at tx and rx, vertex lat lon and the band
        out_writer.writerow( [spot_lines[i,0],  spot_lines[i,1],  spot_lines[i,2],  spot_lines[i,3],  spot_lines[i,4],  spot_lines[i,5], spot_lines[i,6], spot_lines[i,7], spot_lines[i,8], spot_lines[i,9], 
                              spot_lines[i,10], spot_lines[i,11], spot_lines[i,12], spot_lines[i,13], spot_lines[i,14], spot_lines[i,15],
                              band, "-999.9", "-999.9", int(round(rx_azi)), "%.3f" % (rx_lat), "%.3f" % (rx_lon), int(round(tx_azi)), "%.3f" % (tx_lat), "%.3f" % (tx_lon), "%.3f" % (v_lat), "%.3f" % (v_lon)] )
EOF
}

declare UPLOAD_TO_TS="yes"    ### -u => don't upload 

function api_scrape_once() {
    if [[ ! -f ${WSPRNET_SESSION_ID_FILE} ]]; then
        wpsrnet_login
    fi
    if [[ -f ${WSPRNET_SESSION_ID_FILE} ]]; then
        wpsrnet_get_spots
        local ret_code=$?
        if [[ ${ret_code} -ne 0 ]]; then
            [[ ${verbosity} -ge 2 ]] && echo "$(date): api_scrape_once() wpsrnet_get_spots reported error => ${ret_code}."
        else
            wsprnet_to_csv      ${WSPRNET_HTML_SPOT_FILE} ${WSPRNET_CSV_SPOT_FILE}
            wsprnet_add_azi     ${WSPRNET_CSV_SPOT_FILE}  ${WSPRNET_CSV_SPOT_AZI_FILE}
            if [[ ${UPLOAD_TO_TS} == "yes" ]]; then
                wn_spots_batch_upload    ${WSPRNET_CSV_SPOT_AZI_FILE}
            fi
            [[ ${verbosity} -ge 2 ]] && printf "$(date): api_scrape_once() batch upload completed.\n"
        fi
    fi
}

function api_scrape_daemon() {
    [[ ${verbosity} -ge 1 ]] && echo "$(date): wsprnet_scrape_daemon() is starting.  Scrapes will be attempted at second offsets: ${WSPRNET_OFFSET_SECS}"
    setup_verbosity_traps
    while true; do
        api_scrape_once
        api_wait_until_next_offset
   done
}

################### Deamon spawn/status/kill section ##########################################################
declare RUN_AS_DAEMON="yes"   ### -d => change to "no"
function spawn_daemon() {
    local daemon_function=$1
    local daemon_pid_file=$2
    local daemon_log_file=$3
    local daemon_pid=

    if [[ -f ${daemon_pid_file} ]]; then
        daemon_pid=$(cat ${daemon_pid_file})
        if ps ${daemon_pid} > /dev/null ; then
            [[ $verbosity -ge 1 ]] && echo "$(date): spawn_daemon() found running daemon '${daemon_function}' with pid ${daemon_pid}"
            return 0
        fi
        [[ $verbosity -ge 1 ]] && echo "$(date): spawn_daemon() found dead pid file ${daemon_pid_file} for daemon '${daemon_function}'"
        rm ${daemon_pid_file}
    fi
    if [[ ${RUN_AS_DAEMON} == "yes" ]]; then
        ${daemon_function}   > ${daemon_log_file} 2>&1 &
    else
        ${daemon_function} # > ${daemon_log_file} 2>&1 &
    fi
    local ret_code=$?
    local daemon_pid=$!
    if [[ ${ret_code} -ne 0 ]]; then
        [[ $verbosity -ge 1 ]] && echo "$(date): spawn_daemon() failed to spawn ${daemon_function} => ${ret_code}"
        return 1
    fi
    echo ${daemon_pid} > ${daemon_pid_file}
    [[ $verbosity -ge 1 ]] && echo "$(date): spawn_daemon() spawned ${daemon_function} which has pid ${daemon_pid}"
    return 0
}

function status_daemon() {
    local daemon_function=$1
    local daemon_pid_file=$2
    local daemon_pid=

    if [[ -f ${daemon_pid_file} ]]; then
        daemon_pid=$(cat ${daemon_pid_file})
        if ps ${daemon_pid} > /dev/null ; then
            [[ $verbosity -ge 0 ]] && echo "$(date): status_daemon() found running daemon '${daemon_function}' with pid ${daemon_pid}"
            return 0
        fi
        [[ $verbosity -ge 0 ]] && echo "$(date): status_daemon() found dead pid file ${daemon_pid_file} for daemon '${daemon_function}'"
        rm ${daemon_pid_file}
        return 1
    fi
    [[ $verbosity -ge 0 ]] && echo "$(date): status_daemon() found no pid file '${daemon_pid_file}' for daemon'${daemon_function}'"
    return 0
}

function kill_daemon() {
    local daemon_function=$1
    local daemon_pid_file=$2
    local daemon_pid=
    local ret_code

    if [[ ! -f ${daemon_pid_file} ]]; then
        [[ $verbosity -ge 1 ]] && echo "$(date): kill_daemon() found no pid file '${daemon_pid_file}' for daemon '${daemon_function}'"
        ret_code=0
    else
        daemon_pid=$(cat ${daemon_pid_file})
        ps ${daemon_pid} > /dev/null
        ret_code=$?
        if [[ ${ret_code} -ne 0 ]]; then
            [[ $verbosity -ge 1 ]] && echo "$(date): kill_daemon() found dead pid file ${daemon_pid_file} for daemon '${daemon_function}'"
            ret_code=1
        else
            kill ${daemon_pid}
            local ret_code=$?
            if [[ ${ret_code} -ne 0 ]]; then
                [[ $verbosity -ge 1 ]] && echo "$(date): kill_daemon() FAILED 'kill ${daemon_pid}' => ${ret_code} for running daemon '${daemon_function}'"
            else
                [[ $verbosity -ge 1 ]] && echo "$(date): kill_daemon() killed running daemon '${daemon_function}' with pid ${daemon_pid}"
            fi
        fi
    fi
    rm -f ${daemon_pid_file}
    return ${ret_code}
}

declare UPLOAD_LOG_FILE=${WSPRNET_SCRAPER_HOME_PATH}/upload.log
declare UPLOAD_PID_FILE=${WSPRNET_SCRAPER_HOME_PATH}/upload.pid
declare WSPR_DAEMON_LOG_PATH=${WSPRNET_SCRAPER_HOME_PATH}/scraper.log
declare WSPR_DAEMON_PID_PATH=${WSPRNET_SCRAPER_HOME_PATH}/scraper.pid

if [[ ${UPLOAD_MODE} == "API" ]]; then
    declare UPLOAD_DAEMON_FUNCTION=api_scrape_daemon
else
    declare UPLOAD_DAEMON_FUNCTION=oldDb_scrape_daemon
fi

SCRAPER_CONFIG_FILE=${WSPRNET_SCRAPER_HOME_PATH}/wsprnet-scraper.conf
if [[ -f ${SCRAPER_CONFIG_FILE} ]]; then
    source ${SCRAPER_CONFIG_FILE}
fi
declare MIRROR_TO_WD1=${MIRROR_TO_WD1:-no}

function scrape_start() {
    if [[ ${MIRROR_TO_WD1} == "yes" ]]; then
        spawn_daemon         upload_to_wd1_daemon           ${UPLOAD_PID_FILE}      ${UPLOAD_LOG_FILE}
    fi
    spawn_daemon         ${UPLOAD_DAEMON_FUNCTION}      ${WSPR_DAEMON_PID_PATH} ${WSPR_DAEMON_LOG_PATH}
}

function scrape_status() {
    if [[ ${MIRROR_TO_WD1} == "yes" ]]; then
        status_daemon        upload_to_wd1_daemon           ${UPLOAD_PID_FILE}      ${UPLOAD_LOG_FILE}
    fi
    status_daemon        ${UPLOAD_DAEMON_FUNCTION}      ${WSPR_DAEMON_PID_PATH} ${WSPR_DAEMON_LOG_PATH}
}

function scrape_stop() {
    if [[ ${MIRROR_TO_WD1} == "yes" ]]; then
        kill_daemon         upload_to_wd1_daemon           ${UPLOAD_PID_FILE}      ${UPLOAD_LOG_FILE}
    fi
    kill_daemon         ${UPLOAD_DAEMON_FUNCTION}      ${WSPR_DAEMON_PID_PATH} ${WSPR_DAEMON_LOG_PATH}
}

##########################################################################################
### Configure systemctl so the scrape daemon starts during boot
declare -r WSPRNET_SCRAPER_SERVICE_NAME=wsprnet-scraper
declare -r SYSTEMNCTL_UNIT_PATH=/lib/systemd/system/${WSPRNET_SCRAPER_SERVICE_NAME}.service

function setup_systemctl_deamon() {
    local systemctl_dir=${SYSTEMNCTL_UNIT_PATH%/*}
    if [[ ! -d ${systemctl_dir} ]]; then
        echo "$(date): setup_systemctl_deamon() WARNING, this server appears to not be configured to use 'systemnctl' needed to start the kiwiwspr daemon at startup"
        return
    fi
    if [[ -f ${SYSTEMNCTL_UNIT_PATH} ]]; then
        [[ $verbosity -ge 1 ]] && echo "$(date): setup_systemctl_deamon() found this server already has a ${SYSTEMNCTL_UNIT_PATH} file. So leaving it alone."
    fi
    local my_id="scraper"
    local my_group="scraper"
    cat > ${SYSTEMNCTL_UNIT_PATH##*/} <<EOF
    [Unit]
    Description= WsprNet Scraping daemon
    After=multi-user.target

    [Service]
    User=${my_id}
    Group=${my_group}
    Type=forking
    ExecStart=${WSPRNET_SCRAPER_HOME_PATH}/${WSPRNET_SCRAPER_SERVICE_NAME}.sh -a
    ExecStop=${WSPRNET_SCRAPER_HOME_PATH}/${WSPRNET_SCRAPER_SERVICE_NAME}.sh -z
    Restart=on-abort

    [Install]
    WantedBy=multi-user.target
EOF
   mv ${SYSTEMNCTL_UNIT_PATH##*/} ${SYSTEMNCTL_UNIT_PATH}    ### 'sudo cat > ${SYSTEMNCTL_UNIT_PATH} gave me permission errors
   systemctl daemon-reload
   systemctl enable ${WSPRNET_SCRAPER_SERVICE_NAME}.service
   echo "Created '${SYSTEMNCTL_UNIT_PATH}'."
   echo " ${WSPRNET_SCRAPER_SERVICE_NAME} daemon will now automatically start after a powerup or reboot of this system"
}

function enable_systemctl_deamon() {
    if [[ ${USER} != root ]]; then
        echo "This command must be run as user 'root'"
        return
    fi
    setup_systemctl_deamon
    systemctl enable ${WSPRNET_SCRAPER_SERVICE_NAME}.service
}
function disable_systemctl_deamon() {
    systemctl disable ${WSPRNET_SCRAPER_SERVICE_NAME}.service
}

### Prints the help message
function usage(){
    echo "usage: $0  VERSION=$VERSION
    -a             stArt WSPRNET scraping daemon
    -s             Show daemon Status
    -z             Kill (put to sleep == ZZZZZ) running daemon
    -d/-D          increment / Decrement the verbosity of a running daemon
    -e/-E          enable / disablE starting daemon at boot time
    -n             Don't run as daemon (for debugging)
    -u             Don't upload to TS (<S-F10>for debugging)
    -v             Increment verbosity of diagnotic printouts
    -h             Print this message
    "
}

### Print out an error message if the command line arguments are wrong
function bad_args(){
    echo "ERROR: command line arguments not valid: '$1'" >&2
    echo
    usage
}

cmd=bad_args
cmd_arg="$*"

while getopts :aszdDeEnuvh opt ; do
    case $opt in
        a)
            cmd=scrape_start
            ;;
        s)
            cmd=scrape_status
            ;;
        z)
            cmd=scrape_stop
            ;;
        d)
            cmd=increment_verbosity;
            ;;
        D)
            cmd=decrement_verbosity;
            ;;
        n)
            RUN_AS_DAEMON="no"
            ;;
        u)
            UPLOAD_TO_TS="no"
            ;;
        e)
            cmd=enable_systemctl_deamon
            ;;
        E)
            cmd=disable_systemctl_deamon
            ;;
        h)
            cmd=usage
            ;;
        v)
            let verbosity++
            echo "Verbosity = $verbosity" >&2
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit
            ;;
    esac
done

$cmd "$cmd_arg"

