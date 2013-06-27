#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-

set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
SVC_ROOT=/opt/smartdc/wrasse

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh


export PATH=$SVC_ROOT/build/node/bin:$SVC_ROOT/node_modules/.bin:/opt/local/bin:/usr/sbin:/usr/bin:$PATH


function manta_setup_wrasse {
    svccfg import /opt/smartdc/wrasse/smf/manifests/wrasse.xml
    svcadm enable jobpuller || fatal "unable to start jobpuller"
}


# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/wrasse"

manta_common_setup "wrasse"

manta_ensure_zk

echo "Setting up wrasse"
manta_setup_wrasse

manta_common_setup_end

exit 0
