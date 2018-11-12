PATH=$PATH:/spark-bin-dir

// SN
:load scripts_dir/extra.scala
:load scripts_dir/UMLPDC.scala
extract_UMLPDC("stats.txt", "partitions.txt", "mf.txt", "UMLPDC")

extract_CDF_UMLPDC: (in: String, res: String, minDeg: Int, maxDeg: Int, minMsgCnt: Int, maxMsgCnt: Int, pid: Int, minParSize: Int)Unit
extract_CDF_UMLPDC("UMLPDC/UMLPDC_inf", "CDF/CDF_UMLPDC" , 0, 100000, 0, 100000, -1, 4)

:load scripts_dir/extra.scala
:load scripts_dir/UMLPDC.scala
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

:load scripts_dir/extra.scala
extract_UDTMLP("stats.txt", "sid_topic.txt", "tp", "UTMLP", 5)
extract_CDF_UTMLP("UTMLP", "CDF_UTMLP")

extract_UDTMLP("stats.txt", "sid_topic.txt", "tp", "UTDTMLP", 5)
extract_CDF_UTDTMLP("UTDTMLP", "CDF_UTDTMLP")

:load scripts_dir/extra.scala
extract_CDF_UTMLP("UTMLP_150.txt","res_UTMLP_150")
