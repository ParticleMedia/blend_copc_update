#!/bin/bash
export LC=ALL

source "../conf/global.sh"
PYTHON3="/home/services/miniconda3/bin/python3"

function check_status() {
    tag=$1
    if [ $tag -eq 0 ]
    then
        echo "$2 succ"
        return
    else
        echo "Error: checkStatus for $2" | mail -s "[updateCopc] fail!!" ${ALARM_EMAILS}
        echo "$2" >&2
        exit 1
    fi
}


function calc_global_config() {
    ${PYTHON3} -u ${BIN_PREFIX}/generate_param.py ${CURR_DATE} "" "local" ${MID_FILE}
    check_status $? "QratioCopcUpdate: calc_global_config for local ${CURR_DATE}"
    ${PYTHON3} -u ${BIN_PREFIX}/merge_param.py ${MID_FILE} ${OUT_FILE}
    check_status $? "QratioCopcUpdate: mergeParam for local ${CURR_DATE}"

    ${PYTHON3} -u ${BIN_PREFIX}/generate_param.py ${CURR_DATE} "" "nonlocal" ${MID_FILE}
    check_status $? "QratioCopcUpdate: calc_global_config for nonlocal ${CURR_DATE}"
    ${PYTHON3} -u ${BIN_PREFIX}/merge_param.py ${MID_FILE} ${OUT_FILE}
    check_status $? "QratioCopcUpdate: mergeParam for nonlocal ${CURR_DATE}"
}


function calc_exp_config() {
    while read line
    do
        nf_num=`echo "${line}" | awk '{print NF}'`
        if [ ${nf_num} -ne 3 ]
        then
            echo "exp_config_error: ${line}" >&2
            exit 1
        fi
        exp_name=`echo "${line}" | awk '{print $1}'`
        doc_type=`echo ${line} | awk '{print $2}'`
        valid_date=`echo ${line} | awk '{print $3}'`

        curr_date_int=`date -d "${CURR_DATE}" +%s`
        valid_date_int=`date -d "${valid_date}" +%s`

        if [ ${curr_date_int} -le ${valid_date_int} ]
        then
            ${PYTHON3} -u ${BIN_PREFIX}/generate_param.py ${CURR_DATE} ${exp_name} ${doc_type} ${MID_FILE}
            check_status $? "QratioCopcUpdate: calc_exp_config for ${line} ${CURR_DATE}"
            ${PYTHON3} -u ${BIN_PREFIX}/merge_param.py ${MID_FILE} ${OUT_FILE}
            check_status $? "QratioCopcUpdate: mergeParam for ${line} ${CURR_DATE}"
        fi
    done < ${EXP_CONF_FILE}
}

function update_new_config() {
    online_file=${ONLINE_FILE}
    curr_file=${OUT_FILE}
    new_file=${DATA_BK_PREFIX}/qratioCopc.txt.${CURR_DATE}

    if [ ! -f ${online_file} ] || [ ! -f ${curr_file} ]
    then
        echo "${CURR_DATE} file exist/not exist" | mail -s "[updateCopc] fail!!" ${ALARM_EMAILS}
        exit 1
    fi

    ${PYTHON3} -u ${BIN_PREFIX}/check_copc.py ${online_file} ${curr_file}
    check_status $? "QratioCopcUpdate check copc error ${CURR_DATE}"

    cp ${online_file} ${new_file}
    cp ${curr_file} ${online_file}
    echo "${CURR_DATE} succ" | mail -s "[updateCopc] succ!!" ${ALARM_EMAILS}
}



rm ${OUT_FILE} >/dev/null 2>&1
touch ${OUT_FILE}

# calc global config
calc_global_config

# calc exp config
calc_exp_config

# update data
update_new_config
