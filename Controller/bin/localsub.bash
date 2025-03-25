#!/usr/bin/env bash


# mimic submit job to cluster

while getopts ":o:c:" opt; do
  case $opt in
    o) OUTPUTFILE=${OPTARG}
       ;;
    c) CMD=${OPTARG}
       ;;
  esac
done

bash -c "${CMD}" >$OUTPUTFILE & pid=$!

echo "Job <$pid>"
