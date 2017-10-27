//######################################################################################
// Extract User_Topic_Day_Time_Median_Location_Partition for Social Network + Topic + Time Analysis
// Topics are extracted before using a topic modeling algorithm like LDA
// Topics are in a file containing "Statusid	TopicID" named as sid_topic.txt
//######################################################################################
def extract_UTDTMLPDC(stats: String, topics: String, partitions: String, mutual_friends: String, output: String, daySpan: String = "WE/WD", timeSpan: String = "N") = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val SUDL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(0),(x(1), dateparser.parse(x(2)), (x(4).toDouble, x(3).toDouble))))
var SUDTL = SUDL.map({case(s, (u, d, l)) => (s, (u, d.getDay, d.getHours, l))}).map({case(s, (u, d, t, l)) => (s, (u, if(daySpan == "DAILY") d else if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2, l))})
val ST = sc.textFile(topics).map(_.split("\t")).map(x => (x(0), x(1)))
val UDTLT = SUDTL.join(ST).map({case(s,((u, day, time, loc),top)) => (u, (day, time, loc, top))})
val UTDTML = UDTLT.map({case(u,(day, time, loc, top)) => ((u, top, day, time), loc)}).groupByKey().map({case((u, top, day, time), locList) => (u, (top, day, time , geometric_median(locList.toList)))})
val UP = sc.textFile(partitions).map(_.split(",")).map(x => (x(0),x(1)))
val UD = sc.textFile(mutual_friends).map(x => (x.split(",")(0), 1)).reduceByKey(_+_)
val UMC = SUDL.map(x => (x._2._1, 1)).reduceByKey(_+_)
val UTDTMLPDC = UTDTML.join(UP).join(UD).join(UMC) 
UTDTMLPDC.map({case(u, ((((top, day, time, (lat, lon)), p), deg), mc)) => u + "," + top + "," + day + "," + time  + "," + lat + "," + lon + "," + p + "," + deg + "," + mc}).saveAsTextFile(output)
}