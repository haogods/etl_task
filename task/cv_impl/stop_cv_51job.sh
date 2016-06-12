#!/bin/bash

scs=$(screen -ls | grep cv_51job.py_process_ | awk '{print $1}')

for s in $scs
do
	echo $s
    screen -S $s -X quit
done


