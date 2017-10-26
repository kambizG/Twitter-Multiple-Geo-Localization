//######################################################################################
// Extract User_Topic_Day_Time_Median_Location_Partition for Social Network + Topic + Time Analysis
// Topics are extracted before using a topic modeling algorithm like LDA
// Topics are in a file containing "Statusid	TopicID" named as sid_topic.txt
//######################################################################################
def extract_UTDTMLP(stats: String, topics: String , partitions: String, output: String, min_count: Int) ={
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val SUDL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(0),(x(1), dateparser.parse(x(2)), (x(4).toDouble, x(3).toDouble))))
val SUDTL = SUDL.map({case(s, (u, d, l)) => (s, (u, d.getDay, d.getHours, l))}).map({case(s, (u, d, t, l)) => (s, (u, if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2, l))})
val ST = sc.textFile(topics).map(_.split("\t")).map(x => (x(0), x(1)))
val UDTLT = SUDTL.join(ST).map({case(s,((u, day, time, loc),top)) => (u, (day, time, loc, top))})
val valid_users = UDTLT.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > min_count)
val UTDTML = UDTLT.join(valid_users).map({case(u,((day, time, loc, top), x)) => ((u, top, day, time), loc)}).groupByKey().map({case((u, top, day, time), locList) => (u, (top, day, time , geometric_median(locList.toList)))})
val UP = sc.textFile(partitions).filter(x => !x.startsWith("#")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s")).filter(x => !x._2.contains("-")).map(x => (x._2, x._1))
val UTDTMLP = UTDTML.join(UP)
UTDTMLP.map({case(u, ((top, day, time, (lat, lon)), p)) => u + "," + top + "," + day + "," + time  + "," + lat + "," + lon + "," + p}).saveAsTextFile(output)
}

//Test Case
//extract_UTDTMLP("tw_lo.txt", "sid_topic_150.txt", "tp", "UTDTMLP", 5)
