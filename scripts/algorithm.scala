//#################################################################################################
// Social Graph Only
//#################################################################################################
:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
extract_UMLP("tw_lo.txt", "tp_louvain", "UMLP", 5)

:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
extract_UMLP("st_all_uniq.txt", "result/tp", "UMLP", 10)

:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
extract_UMLP("stats.txt", "mf_partitions/tp", "UMLP", 6)

:load /home/kambiz/data/tw_data_all_clean/script/extra.scala
val UMLP = sc.textFile("UMLP.txt").map(_.split(",")).map(x => (x(0), ((x(1).toDouble,x(2).toDouble), x(3))))
val split = UMLP.map({case(u,(ml,p)) => (p,u)}).groupByKey().filter(_._2.size > 4).map({case(p,u) => (p, u.splitAt((u.size * 0.3).toInt))})
val train = split.map({case(p,(tr,ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p,(tr,ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PML = UMLP.join(train).map({case(u,((ml, p),x)) => (p, ml)}).groupByKey().map({case(p, ls) => (p, geometric_median(ls.toList))})
val U_PE = UMLP.join(test).map({case(u, ((ml,p),x)) => (p, (u, ml))}).join(PML).map({case(p, ((u, ml), pl)) => (u, geoDistance_points(ml, pl))})
val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).collect
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)
val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
temp1.union(temp2).reduceByKey(_+_).map(x => (x._1 + "\t" + x._2)).saveAsTextFile("res_UMLP")

//#################################################################################################
// Social graph + Time
//#################################################################################################
:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
extract_UDTMLP("st_all_uniq.txt", "result/tp", "UDTMLP", 10)

:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
extract_UDTMLP("tw_lo.txt", "tp_louvain", "UDTMLP_1H", 5)

:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala

def extract_CDF_UDTMLP(in: String, res: String) = {
val UDTMLP = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), x(2), (x(3).toDouble,x(4).toDouble), x(5))))
val split = UDTMLP.map({case(u,(d,t, ml,p)) => (p,u)}).groupByKey().filter(_._2.size > 4).map({case(p,u) => (p, u.splitAt((u.size * 0.2).toInt))})
val train = split.map({case(p,(tr,ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p,(tr,ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PDTML = UDTMLP.join(train).map({case(u,((d, t, ml, p),x)) => ((p, d, t), ml)}).groupByKey().map({case(pdt, ls) => (pdt, geometric_median(ls.toList))})
val U_PE = UDTMLP.join(test).map({case(u, ((d, t, ml, p),x)) => ((p, d, t), (u, ml))}).join(PDTML).map({case(pdt, ((u, ml), pml)) => ((u,pdt), geoDistance_points(ml, pml))})
val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).collect
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)

val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
temp1.union(temp2).reduceByKey(_+_).map(x => (x._1 + "\t" + x._2)).saveAsTextFile(res)
}

extract_CDF_UDTMLP("UDTMLP.txt", "res_UDTMLP")
extract_CDF_UDTMLP("UDTMLP_1H.txt", "res_UDTMLP_1H")
extract_CDF_UDTMLP("UDTMLP_3H.txt", "res_UDTMLP_3H")

//#################################################################################################
// Social graph + Time + Text
//#################################################################################################

//##################
//read stats, clean, remove stopwords, write for topic extraction
//##################
:load /home/kambiz/data/tw_data_all_clean/tw_location_identification/scripts/extra.scala
val sw = sc.textFile("../longstoplist.txt").collect
val stats = sc.textFile("tw_lo.txt").map(_.split(",",7)).map(x => (x(0), x(6)))
val cleanStats = stats.map(x => (x._1, cleanRemoveStopWords(x._2, sw, 2, 15))
cleanStats.map(x => x._1 + "," + x._2).saveAsTextFile("stats_clean")
//##################



val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val stats = sc.textFile("tw_lo.txt").map(_.split(",",7)).map(x => (x(1),(dateparser.parse(x(2)).getDay, if(dateparser.parse(x(2)).getHours - 2 < 6) 0 else if (dateparser.parse(x(2)).getHours - 7 < 12) 1 else 2, x(4).toDouble, x(3).toDouble, x(6)))).filter(x => x._2._1 > 0 && x._2._1 < 6)

//val real_users = sc.textFile("fr_fw_lo_cnd.txt").map(_.trim.split("\\s")).map(x => (x(1), x(0).toLong)).filter(_._2 > 10)
//val valid_users = stats.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > 20).join(real_users)
val valid_users = stats.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > 5)
val valid_stats = stats.join(valid_users).map(x => (x._1, x._2._1))

val partitions = sc.textFile("tp").filter(x => !x.startsWith("#")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s")).filter(x => !x._2.contains("-")).map(x => (x._2, x._1))
val user_partition = partitions.join(valid_users).map(x => (x._1, x._2._1))
// (UserID, PartitionID)
val splitPartition = user_partition.map(x => (x._2,x._1)).groupByKey().filter(_._2.size > 4).map(x => (x._1, x._2.splitAt((x._2.size * 0.8).toInt)))
val train = splitPartition.map(x => (x._1, x._2._1)).flatMapValues(x => x).map(x => (x._2, x._1))
// (UniqUserIDTrain, PartitionID)
val test = splitPartition.map(x => (x._1, x._2._2)).flatMapValues(x => x).map(x => (x._2, x._1)).reduceByKey((a,b) => a)
// (UniqUserIDTest, PartitionID)
val user_loc = valid_stats.join(test).map(x => ((x._1, x._2._1._1, x._2._1._2),((x._2._1._3, x._2._1._4),x._2._1._5))).groupByKey().map(x => (x._1, geoMetric_median_with_text(x._2.toList))).map(x => (x._1._1, x))
//user_loc.map(x => (x._1, 1)).reduceByKey(_+_).map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._1).collect
val part_loc = train.join(stats).map(x => ((x._2._1, x._2._2._1, x._2._2._2),((x._2._2._3, x._2._2._4), x._2._2._5))).groupByKey().map(x => (x._1, geoMetric_median_with_text(x._2.toList))).map(x => (x._1._1, x))
//user_partition.join(user_loc).map(_._2).join(part_loc).map(_._2).first
val user_prediction = user_partition.join(user_loc).map(_._2).join(part_loc).map(_._2)
val predictionByDate = user_prediction.filter({case(((id, d, h),(info)), ((pid, pd, ph), (pinfo))) => d == pd && h==ph})
//val predictionByHour = user_prediction.filter({case(((id, d, h),(info)), ((pid, pd, ph), (pinfo))) => h==ph})

{
 val words1 = d1.split("\\s")
 val words2 = d2.split("\\s")
 var count_sim = 0.0
 var count = 0.0
 for(w1 <- words1){
  for(w2 <- words2){
    count += 1
    if(w1.equals(w2))
      count_sim += 1
  }}
 return count_sim/count
}

val date_prediction = predictionByDate.map({case(((id, d, h),(loc, txt)), ((pid, pd, ph), (ploc, ptxt))) => (id, (loc, ploc, txt, ptxt))})
val text_prediction = date_prediction.map({case(id, (loc, ploc, txt, ptxt))=> (id, (loc, ploc, similarity(txt, ptxt)))}).reduceByKey((a,b) => if(a._3 > b._3) a else b)
val prediction_error = text_prediction.map({case(id, (loc, ploc, sim)) => geoDistance(loc._1, loc._2, ploc._1, ploc._2)})
val user_prediction_error = text_prediction.map({case(id, (loc, ploc, sim)) => (id, geoDistance(loc._1, loc._2, ploc._1, ploc._2))})
val AED = prediction_error.map({case(dist) => (1, (dist, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).collect
val cnt = (prediction_error.count / 2).toInt
val MED = prediction_error.sortBy(x => x).take(cnt).drop(cnt -1)

