#!/bin/bash
type=3
distributionType=0
testCount=5
startPower=22
stopPower=29 
N=30
#k=33554431
beta=2
declare -a arr=("result.out" "baseline_filter_shuffle" "baseline_filter" "baseline" "U_K16" "U_K31" "U_K64" "U_K128" "U_K256" "U_K512")
rm output.txt
#declare -a k=(1 2 4 8 16 31 64 128 256 512)
#for (( beta=2; beta<3; beta=beta+1 ))
for (( k=1; k<=1; k=k+1 ))
do
	for (( N=1; N<=200; N=N+1 ))
	do
		./ceci dataset/youtube/query_graph/query_dense_8_$N.graph dataset/youtube/data_graph/youtube.graph #>> ${arr[0]}
		echo -n "Finished"
        echo $N
        #./topk.bin  29 33554431 8 2
	done
done
echo -n "Finihed processing for a k "
printf "\n"
#diff <(grep ':' result.out | sed 's/^.*://')  <(grep ':' truth.out | sed 's/^.*://')

