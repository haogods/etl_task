#!/bin/bash

filename=`ls -l | awk '{print $9}'`
for f in $filename
do
    new_name=`echo $f | sed 's/^[^0-9]\+//g'`
    echo $new_name
    
done

