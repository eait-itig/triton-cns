#!/usr/bin/env bash

display_usage() {
	echo -e "Usage:\n $0 [ custom | servers ]";
	exit 1
}

if [ $# -ne 1 ]
then
	display_usage
fi

NODE=/opt/triton/cns/build/node/bin/node
CUSTOM=/opt/triton/cns/eait_add_custom.js
SERVERS=/opt/triton/cns/eait_add_servers.js

if [ "$1" = "custom" ]; then
	$NODE $CUSTOM
elif [ "$1" = "servers" ]; then
	$NODE $SERVERS
else
	display_usage
fi
