:load /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/extra.scala
// SN
extract_UMLPDC("stats.txt", "partitions.txt", "mf.txt", "UMLPDC")
extract_CDF_UMLPDC("UMLPDC.txt", "CDF_UMLPDC", 100, 5)
// SN + Time
extract_UDTMLPDC("stats.txt", "partitions.txt", "mf.txt", "UDTMLPDC", "N")
extract_UDTMLPDC("stats.txt", "partitions.txt", "mf.txt", "UDTMLPDC", "3H")
extract_UDTMLPDC("stats.txt", "partitions.txt", "mf.txt", "UDTMLPDC", "1H")
extract_CDF_UDTMLPDC("UDTMLPDC.txt", "CDF_UDTMLPDC", 100, 5)

//#################################################################################################
// Social graph + Text
//#################################################################################################
// Extract topics from status
// param: number_of_topics = {e.g., 200}
// sh ../../tw_loi/scripts/topic_model/extract_topics.sh 200

:load /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/extra.scala
extract_UDTMLP("stats.txt", "sid_topic.txt", "tp", "UTMLP", 5)
extract_CDF_UTMLP("UTMLP", "CDF_UTMLP")

extract_UDTMLP("stats.txt", "sid_topic.txt", "tp", "UTDTMLP", 5)
extract_CDF_UTDTMLP("UTDTMLP", "CDF_UTDTMLP")

:load /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/extra.scala
extract_CDF_UTMLP("UTMLP_150.txt","res_UTMLP_150")
