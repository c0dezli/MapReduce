#!/bin/bash


thread_num=$1
output_=$2
total=0
avg=0
std=0


for i in `seq 1 20`;  
do
./mr-wordc thread_num | output_

#./mr-wordc thread_num output_
#./testtest.c output_
#array[i-1]=output_
let "array[i-1]=output_"
#total+=output_
let "total+=output_"
done


let "avg=total/20"
echo $avg

for i in `seq 1 20`;
do
let "s1=array[i-1]-avg"
let "s2=s1*s1"
let "sarray[i-1]=s2"
done

for i in `seq 1 20`;
do
let "total+=sarray[i-1]"
done

let "std=total/20"
echo $std

