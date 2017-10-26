PATH=$PATH:/home/kambiz/data/tw_data_all_clean/clustering_programs_5_2/bin:/home/kambiz/spark-1.6.2/bin/
if [ $# -eq "0" ]
then
	echo "Number of topics is required!"
else
	nTopics=$1
	spark-shell --driver-memory 20g -i /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/topic_model/clean_documents.scala
	cat stats_clean/part* >> stats_clean.txt
	rm -r stats_clean
	mkdir LDA
	cat stats_clean.txt | cut -d',' -f2 >> LDA/doc_info.txt
	cnt="$(wc -l LDA/doc_info.txt | awk '{print $1}')"
	sed -i "1s/^/$cnt\n/" LDA/doc_info.txt
	cat stats_clean.txt | cut -d',' -f1 >> sids.txt
	rm stats_clean.txt
	DIR="$(pwd)"
	LDA_DIR=$DIR"/LDA/"
	cd /home/kambiz/JGibbLDA-v.1.0/
	java -mx50g -cp bin:lib/args4j-2.0.6.jar jgibblda.LDA -est -alpha 0.05 -beta 0.01 -ntopics $nTopics -niters 5 -savestep 501 -twords 0 -dir $LDA_DIR -dfile doc_info.txt
	cd $DIR
	spark-shell --driver-memory 20g -i /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/topic_model/convert_topics.scala
	cat topics/part-0* >> topics.txt
	paste sids.txt topics.txt >> sid_topic.txt
	rm -r LDA/
	rm -r topics/
	rm -r sids.txt
	rm topics.txt
fi
