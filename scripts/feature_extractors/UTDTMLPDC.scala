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

//######################################################################################
// Extract CDF for combination of all features including SN, Time and Topic
//######################################################################################
def extract_CDF_UTDTMLPDC(in: String, res: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue, day: Int = -1, time: Int = -1, pid: Int, minParSize: Int) = {
val ML = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), x(2), x(3), (x(4).toDouble, x(5).toDouble), x(6), x(7).toInt, x(8).toInt)))
var ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
if(day != -1)
 if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day && t.toInt == time})
 else
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day})
else if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && t.toInt == time})

var UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))})
if(pid != -1)
 UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))}).filter({case (u, (top, d, t, ml, p)) => p.toInt == pid})

val split = UTDTMLP.map({case(u, (top, d, t, ml, p)) => (p, u)}).groupByKey().map(x => (x._1, x._2.toList.distinct)).filter(_._2.size > minParSize).map({case(p, ul) => (p, ul.splitAt((ul.size * 0.7).toInt))})
val train = split.map({case(p, (tr, ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p, (tr, ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PTDTML = UTDTMLP.join(train).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), ml)}).groupByKey().map({case(ptdt, ls) => (ptdt, geometric_median(ls.toList))})
val U_PE = UTDTMLP.join(test).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), (u, ml))}).join(PTDTML).map({case(ptdt, ((u, ml), pml)) => ((u, ptdt), geoDistance_points(ml, pml))})
val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).first.toDouble
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)(0).toDouble
val Recall = (U_PE.count * 1.0/UTDTMLP.join(test).count).toDouble

val pw = new java.io.PrintWriter(new java.io.File(res + "_values.txt"))
pw.write("AED\t" + AED + "\n")
pw.write("MED\t" + MED + "\n")
pw.write("REC\t" + Recall + "\n")
pw.close

val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
val temp3 = temp1.union(temp2).reduceByKey(_+_).sortBy(_._1)
temp3.map(x => (0, x)).groupByKey().map(x => CDF(x._2.toList)).flatMap(x => x).map(x => x._2).saveAsTextFile(res)
}

//######################################################################################
// Extract and returns MED for combination of all features
//######################################################################################
def extract_MED_UTDTMLPDC(in: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue, day: Int = -1, time: Int = -1, pid: Int, minParSize: Int): (Double, Double) = {
val ML = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), x(2), x(3), (x(4).toDouble, x(5).toDouble), x(6), x(7).toInt, x(8).toInt)))
var ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
if(day != -1)
 if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day && t.toInt == time})
 else
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day})
else if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && t.toInt == time})

var UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))})
if(pid != -1)
 UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))}).filter({case (u, (top, d, t, ml, p)) => p.toInt == pid})

val split = UTDTMLP.map({case(u, (top, d, t, ml, p)) => (p, u)}).groupByKey().map(x => (x._1, x._2.toList.distinct)).filter(_._2.size > minParSize).map({case(p, ul) => (p, ul.splitAt((ul.size * 0.7).toInt))})
val train = split.map({case(p, (tr, ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p, (tr, ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PTDTML = UTDTMLP.join(train).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), ml)}).groupByKey().map({case(ptdt, ls) => (ptdt, geometric_median(ls.toList))})
val U_PE = UTDTMLP.join(test).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), (u, ml))}).join(PTDTML).map({case(ptdt, ((u, ml), pml)) => ((u, ptdt), geoDistance_points(ml, pml))})
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)(0).toDouble
val Recall = (U_PE.count * 1.0/UTDTMLP.join(test).count).toDouble
return (MED, Recall)
}


