# Some color code definitions
RED=
GREEN=
YELLOW=
BLUE=
BLINK=
NC=

# To detect if stdout or stderr is being attached to terminal
function is_tty {
    [[ -t 1 ]] && [[ -t 2 ]]
    return $?
}

# Enable colorful output only if no terminal attached
is_tty && RED=$(echo -e "\001\033[1;31m\002")
is_tty && GREEN=$(echo -e "\001\033[1;32m\002")
is_tty && YELLOW=$(echo -e "\001\033[1;33m\002")
is_tty && BLUE=$(echo -e "\001\033[1;34m\002")
is_tty && BLINK=$(echo -e "\001\033[5m\002")
is_tty && NC=$(echo -e "\001\033[0m\002")


function INFO {
    echo "${GREEN}[INFO]${NC}" $@
}


function WARN {
    echo "${YELLOW}[WARN]${NC}" $@
}


function ERROR {
    echo "${RED}[ERROR]${NC}" $@ >&2
}


function ERROR_AND_EXIT {
    ERROR $@
    exit 1
}


# Tell if a path is an absolute one
# args: <path>
function is_absolute_path {
    [[ ${1} = /* ]]
    return $?
}


# Tell if a path is an existing directory
# args: <path>
function is_existing_dir {
    [[ -d ${1} ]]
    return $?
}


# Tell if a process is running, via a file hosting its pid
# args: <path to pid file>
function is_process_running {
    local pid_file=${1}
    [[ -f ${pid_file} ]] || return 1

    local pid=$(cat ${pid_file})
    [[ -z ${pid} ]] && return 1

    ps -p ${pid} &>/dev/null
    return $?
}


# Tell if some process is listening on the target port
# args: <port number>
function is_port_listened_on {
    local port=${1}
    if which ss &>/dev/null; then
        ss -lnt | grep ${port} &>/dev/null && return 0
    fi
}


# To wait for a process to exit
# args: <path to pid file> <seconds to wait>
function wait_for_exit {
    local pid_file=${1}
    local seconds=${2}
    is_process_running ${pid_file} || return 0
    while [[ ${seconds} > 0 ]]; do
        sleep 0.1
        is_process_running ${pid_file} || return 0
        seconds=$(echo "${seconds} - 0.1" | bc -l)
    done
}

# To read a config item's value from the config file
# args: <config file> <config item name>
function get_item_from_config {
    sed -n -e "s/^--${2}=\(.*\)$/\1/p" ${1} 2>/dev/null | sed -n "1p"
}


# To read the path to the pid file from the config file
# args: <config file>
function get_pid_file_from_config {
    get_item_from_config ${1} "pid_file"
}


# To read to listen port number from the config file
# args: <config file>
function get_port_from_config {
    get_item_from_config ${1} "port"
}


# To read logging directory from the config file
# args: <config file>
function get_log_dir_from_config {
    get_item_from_config ${1} "log_dir"
}


# To create the logging directory if absent
# args: <path to directory>
function make_log_dir_if_absent {
    [[ -d ${1} ]] || mkdir -p ${1} >/dev/null
}


# Perform some environment checking
function env_check {
    local nfile=$(ulimit -n 2>/dev/null)
    local core=$(ulimit -c 2>/dev/null)
    local cputime=$(ulimit -t 2>/dev/null)

    if [[ ${nfile} -le 1024 ]]; then
        WARN "The maximum files allowed to open might be too few: ${nfile}"
    fi

    if [[ ${core} -eq 0 ]]; then
        ulimit -c unlimited &>/dev/null
    fi

    if [[ ${cputime} != "unlimited" ]]; then
        WARN "The CPU time a process can consume is restricted to ${cputime}"
    fi
}
