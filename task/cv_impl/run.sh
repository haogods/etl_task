#!/bin/bash
#coding=utf-8

my_python=`which python`

function print_usage(){

    echo "Usage: run.sh [cv_51job|cv_zhilian] thread_cnt process_cnt"
}

function check_int(){
    echo $1
    expr $1 + 0 &>/dev/null
    if [ $? -ne 0 ]; then
        print_usage;
        exit 1;
    fi

}


if [ $# != 3 ]; then
    print_usage;
    exit 1;
fi
check_int $2;
check_int $3;

echo $1

for((i=0;i<$3;i++))
do
    cmd="screen -LdmS $1.py_process_$i $my_python $1.py $2 $i $3"
    echo ${cmd}
    $cmd
done

