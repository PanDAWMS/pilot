#!/bin/bash

function ignore {
    echo ignoring signal
    sleep 10
}

trap ignore SIGHUP SIGINT SIGTERM

echo out
echo err>&2
sleep 10
