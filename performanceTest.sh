#!/bin/bash


#arg1_=$2_
#output_=$2
total=0
avg=0
std=0


for i in `seq 1 20`;  
do
#$1 arg1_ output_
#./testtest.c output_
#array[i-1]=output_
array[i-1]=6
#total+=output_
total+=6
done


avg=total/20
echo $avg

for i in `seq 1 20`;
do
s1=array[i-1]-avg
s2=s1*s1
sarray[i-1]=s2
done

for i in `seq 1 20`;
do
total+=sarray[i-1]
done

std=total/20
echo $std

