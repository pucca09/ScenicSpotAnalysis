#!/bin/bash
a="HaiNan"
b="201508"
cd /extdisk/wtist/tele/mobility/python
source path.sh
python pipeline/run_ScenicSpotAnalysis.py --conf=conf/ScenicAnalysis.conf --province=$a --month=$b
pathfolder="/user/tele/trip/BackEnd/ScenicSpotAnalysis/"${a}"/"${b}"/ForeEndVisData"
folderlist=($(hdfs dfs -ls $pathfolder | awk -F ' ' '{print$8}'))
folderNum=${#folderlist[@]}
localdir="/extdisk/wtist/tele/Chenqingqing/ForeEndVisData/"${b}
if [ -d "$localdir" ];then
        rm -rf $localdir/*
else
        mkdir $localdir
fi

for ((i=0;$i<$folderNum;i++))
do
        folder=${folderlist[i]}
        name=$(basename ${folder})
        echo $name
        localfile=${localdir}"/"${name}
        hdfs dfs -text $folder/* > $localfile
done
scp /extdisk/wtist/tele/Chenqingqing/ForeEndVisData/${b}/* root@123.56.71.119:/home/root/webdisplay/public/source/

