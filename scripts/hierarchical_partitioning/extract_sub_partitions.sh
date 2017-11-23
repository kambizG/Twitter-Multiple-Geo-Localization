PATH=$PATH:/home/kambiz/data/tw_data_all_clean/clustering_programs_5_2/bin:/home/kambiz/data/spark-2.2.0-bin-hadoop2.7/bin/
if [ $# -eq "0" ]
then
echo "Max Partition Size is required!"
else
	max_comm_size=$1
        echo "####################    Set Parameters     ########################"
        echo $max_comm_size > max_comm_size.txt
        cat max_comm_size.txt
        echo "#################    Initial Partitioning     #####################"
        louvain_method -f network -r 5 > clout
        echo "###################    Begin Iterations     #######################"
        v="1"
        while [ ! $v = "0" ]
        do
                echo "################    Extract Sub-Partitions     ####################"
                spark-shell --driver-memory 120g -i /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/hierarchical_partitioning/extract_sub_partitions.scala
                echo "###############    Partition Sub-Partitions     ###################"
                rm tp
                v="$(head -1 count.txt)"
                echo $v
                if [ ! $v = "0" ]; then
                        cd new_partitions
                        for D in *; do cd $D; cat part-0* >> $D; rm part-0*; rm _SUCCESS; louvain_method -f $D > clout ; cd ..; done
                        cat */tp >> ../tp
                        cd ..
                        rm -r new_partitions/
                fi
                echo "###############    Aggregate Sub-Partitions     ###################"
                cat tp_s*/part* >> small_tp
                        rm -r tp_s*
                rm count.txt
        done
        echo "###############    Final Cleaning     ###################"
        cat small_tp >> tp
        spark-shell --driver-memory 120g -i /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/hierarchical_partitioning/extract_id_partitions.scala
        cat partitions_$max_comm_size/part* >> partitions/partitions_$max_comm_size.txt
        rm -r partitions_$max_comm_size/
        rm small_tp
        rm tp
        rm time_seed.dat
        rm clout
        rm max_comm_size.txt
	rm short_tp1
	rm tp1
fi
