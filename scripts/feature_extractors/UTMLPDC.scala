//######################################################################################
// Extract User_Topic_Median_Location_Partition for Social Network + Topic Analysis
// Topics are extracted before using a topic modeling algorithm like LDA
// Topics are in a file containing "Statusid	TopicID" named as sid_topic.txt
//######################################################################################
def extract_UTMLP(stats: String, topics: String , partitions: String, output: String, min_count: Int) ={
val SUL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(0), (x(1), (x(4).toDouble, x(3).toDouble))))
val ST = sc.textFile(topics).map(_.split("\t")).map(x => (x(0), x(1)))
val ULT = SUL.join(ST).map({case(s,((u,loc),top)) => (u, (loc, top))})
val valid_users = ULT.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > min_count)
val UTML = ULT.join(valid_users).map({case(u,((loc,top),x)) => ((u, top), loc)}).groupByKey().map({case((u, top),locList) => (u,(top, geometric_median(locList.toList)))})
val UP = sc.textFile(partitions).filter(x => !x.startsWith("#")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s")).filter(x => !x._2.contains("-")).map(x => (x._2, x._1))
val UTMLP = UTML.join(UP)
UTMLP.map({case(u, ((top, (lat, lon)), p)) => u + "," + top + "," + lat + "," + lon + "," + p}).saveAsTextFile(output)
}
