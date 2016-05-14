#!/usr/bin/env bash

salat3=salat3.eecs.umich.edu
db_path=/home/jiamin/speculative/leveldb/client
output_path=/home/jiamin/speculative/out

exp_name="diff_c_p"

ssh salat3 "mkdir -p ${output_path}/"
ssh salat3 "touch ${output_path}/${exp_name}"

for p in `seq 0 5`;
do
    for((c=1;c<=128;c*=2))
    do
        ssh salat3 "echo \"${p},${c}\" >> ${output_path}/${exp_name}"
        ssh salat3 "${db_path}/glakv_server -p ${p} -n 1 >> ${output_path}/${exp_name}&"
        ${db_path}/glakv_client -e -s 1000000 -c ${c} -n 100000
    done
done
