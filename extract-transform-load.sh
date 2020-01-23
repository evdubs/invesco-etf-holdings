#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")
current_year=$(date "+%Y")

racket ${dir}/extract.rkt
racket ${dir}/transform-load-csv.rkt -p "$1"

7zr a /var/tmp/invesco/etf-holdings/${current_year}.7z /var/tmp/invesco/etf-holdings/${today}
