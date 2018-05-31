#!/bin/bash

set -euo pipefail

NODES_FILE="$HOME/code/jepsen/nodes"
YB_PEM_FILE="$HOME/.yugabyte/yugabyte-dev-aws-keypair.pem"
SSH_PORT=54422
PKG_LIST="wget curl vim man-db faketime ntpdate unzip iptables psmisc tar bzip2 libzip2 "\
"iputils-ping iproute rsyslog logrotat"

IFS=$'\r\n' GLOBIGNORE='*' NODES=($(cat $NODES_FILE))

SSH_COMMON_OPTS=(-i $YB_PEM_FILE)
SSH_OPTS=(${SSH_COMMON_OPTS[@]} -p $SSH_PORT)
SCP_OPTS=(${SSH_COMMON_OPTS[@]} -P $SSH_PORT)
if [[ "$(uname -s)" == "Darwin" ]]; then
  IS_MACOS=1
else
  IS_MACOS=0
fi

for node in "${NODES[@]}"
do
  if [[ "$IS_MACOS" == "1" ]]; then
    ip=$(dscacheutil -q host -a name $node | grep ip_address | awk '{print $2}')
  else
    ip=$(getent hosts $node | awk '{print $1}')
  fi
  sed -i.bak "/$node/d" ~/.ssh/known_hosts
  sed -i.bak "/$ip/d" ~/.ssh/known_hosts

  # Make sure that the StrictHostChecking does not create any issue with the later commands.
  ssh ${SSH_OPTS[@]} centos@$node 'hostname'

  # Install required packages.
  ssh ${SSH_OPTS[@]} centos@$node "sudo yum install -y $PKG_LIST"

  # Allow yugabyte user key-based access.
  (echo && ssh-keygen -y -f $YB_PEM_FILE) | \
    ssh ${SSH_OPTS[@]} centos@$node 'sudo sh -c "cat >>/home/yugabyte/.ssh/authorized_keys"'

  # Allow passwordless sudo for yugabyte user.
  ssh ${SSH_OPTS[@]} centos@$node \
      'sudo sh -c "echo yugabyte ALL=\(ALL:ALL\) NOPASSWD:ALL >/etc/sudoers.d/yugabyte_sudoers"'

  ssh ${SSH_OPTS[@]} yugabyte@$node 'hostname'
done
