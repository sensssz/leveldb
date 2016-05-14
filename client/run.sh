#!/usr/bin/env bash

export LD_LIBRARY_PATH=/home/jiamin/gcc/lib64:/home/jiamin/tmux/lib:/home/jiamin/usr/lib:/home/jiamin/mysql/lib:$LD_LIBRARY_PATH

salat3=salat3.eecs.umich.edu
db_path=/home/jiamin/speculative/leveldb/client
output_path=/home/jiamin/speculative/out

exp_name="diff_c_p"

trap 'quit=1' INT

ssh salat3 "mkdir -p ${output_path}/"
ssh salat3 "rm ${output_path}/${exp_name} && touch ${output_path}/${exp_name}"

for p in `seq 0 5`;
do
    for((c=1;c<=128;c*=2))
    do
        if [[ -z ${quit+x} ]]; then
            exit 0
        fi
        ssh salat3 /bin/zsh << EOF
        export LD_LIBRARY_PATH=/home/jiamin/gcc/lib64:/home/jiamin/usr/lib:$LD_LIBRARY_PATH
        echo "${p},${c}" >> ${output_path}/${exp_name}
        ${db_path}/glakv_server --dir ${db_path}/glakv_home -p ${p} -n 1 >> ${output_path}/${exp_name}&
EOF
        sleep 2
        ${db_path}/glakv_client -e -s 1000000 -c ${c} -n 50000
        sleep 1
    done
done
