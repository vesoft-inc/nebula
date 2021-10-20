#! /bin/bash

src=
dst=
metaDirName=
configs=

function usage {
Usage="this tool is a simple wrapper to scp to copy local folder of metad to another (remote)place \n \
    it afford 4 options:    \n \
        1. -f (from) local metad dir \n \
        2. -t (to) remote destination \n \
        3. -u (update) any configs need to be changed \n \
            different configs should be seperated by ':' \n \
            each config has to be the form of "local_ip=172.0.0.1" \n \
        \n \
    for example \n \
        ./meta-transfer-tools.sh -f /path-to-src/metad1 -t bob@172.0.0.1:/path-to-dst -u local_ip=172.0.0.1:port=10086 \n \
    "


echo -e $Usage
}

while getopts ":hf:t:u:" opt; do
    case $opt in
        h)
            usage
            exit 0
            ;;
        f)
            echo "trying to copy metad from: " $OPTARG
            src=$OPTARG
            metaDirName=${OPTARG##*/}
            ;;
        t)
            echo "trying to copy metad to: " $OPTARG
            dst=$OPTARG
            ;;
        u)
            echo "-u: " $OPTARG
            configs=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [[ ! -d ${src} ]]; then
    echo "${src} does not exist or not a folder"
    exit 1
fi

configSurfix=/etc/nebula-metad.conf
if [[ ! -f ${src}${configSurfix} ]]; then
    echo "${src}${configSurfix} does not exist"
    exit 1
fi

tmpConfigFile=nebula-metad.conf.bak

cp ${src}${configSurfix} ${tmpConfigFile}
if [[ -z $configs ]]; then
    echo no configs need to replace
else
    echo going to parse configs: $configs
    array=(${configs//:/ })
    for var in ${array[@]}
    do
        key=${var%=*}
        val=${var#*=}
        sed -iE "s/--${key}.*$/--${key}=${val}/" ${tmpConfigFile}
    done
fi

echo "trying to copy metad from $src to $dst"

set -x
scp -r $src $dst
scp $tmpConfigFile $dst/$metaDirName$configSurfix
set +x

rm $tmpConfigFile*
