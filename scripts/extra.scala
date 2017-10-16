//######################################################################################
// Preprocessing
// Clean Documents and Remove stopwords using the longstopwords.txt
//######################################################################################
def cleanRemoveStopWords(document: String, sw: Array[String], minLen: Integer, maxLen: Integer): String = {
var words = document.trim.toLowerCase().replaceAll("[!\"“”$%&'*+,./:;<=>?\\[\\]^`{\\|}~()]", " ").replaceAll("http", "").replaceAll("\\\\", "").replaceAll("\\s+", " ").split("\\s")
var res = ""
for(w <- words){
if(w.length > minLen && w.length < maxLen && !sw.contains(w))
res += w + " "
}
return res.trim
}

//######################################################################################
// Extract Location Count Frequency
//####################################################################################
def extract_location_count_frequency(status_File: String, outPut: String) ={
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val stats = sc.textFile(status_File).map(_.split(",",6)).map(x => (x(1), x(4).toDouble, x(3).toDouble))
val loc_freq = stats.map(x => (x,1)).reduceByKey(_+_).map(x => (x._1._1, 1)).reduceByKey(_+_).map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._1)
loc_freq.map(x => x._1  + "," + x._2).saveAsTextFile(outPut)
}

//######################################################################################
// Extract Ego Network By User_Id
//######################################################################################
def extract_ego(uid: String, mutual_friends: String, outPut: String) = {
val friends = sc.textFile(mutual_friends).map(_.split(",")).map(x => (x(0), x(1)))
val mf = friends.map(x => (x._2, x._1)).union(friends)
val direct_friends = mf.filter(_._1 == uid)
val fr_fr = direct_friends.map(x => (x._2, x._1)).join(mf).map(x => (x._1, x._2._2)).filter(_._2 != uid)
val egoNet = fr_fr.map(x => (x._2, x._1)).join(direct_friends.map(x => (x._2, x._1))).map(x => (x._2._1, x._1))
direct_friends.union(egoNet).map(x => x._1 + "," + x._2).saveAsTextFile(outPut)	
}

//Test Case
//extract_ego("17057819", "mutual_friends.txt", "ego_17057819")

//##############################################################################################################
// Check Temporal Distribution on different partitions using Leuven Partitioning.
//##############################################################################################################
//The community detection algorithm to partition the graph into multiple communities
// python /home/kambiz/data/tw_data_all_clean/clustering_programs_5_2/select.py -n mf_lo.txt -p 4 -f result -c 1

def top_n_partitions(n: Int, partition: String) = {
val parts = sc.textFile(partition).filter(x => x.matches("^[0-9].*")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s"))
parts.groupByKey().map(x => (x._1, x._2.size)).sortBy(_._2,false).take(n)
}

def partition_temporal_distribution(pid: Long, stats_file: String, partitions: String) = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val stats = sc.textFile(stats_file).map(_.split(",",7)).map(x => (x(1),(dateparser.parse(x(2)).getHours)))
val parts = sc.textFile(partitions).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s"))
val par_n = parts.filter(_._1 == pid).map(x => (x._2, x._1))
val sta_n = stats.join(par_n).map(x => (x._2._1, 1))
val count = sta_n.count.toDouble
val hourD = sta_n.reduceByKey(_+_).map(x => (x._1, x._2 * 1.0 / count)).sortBy(_._1)
hourD.map(_._2).collect
}
//Test Case
//top_n_partitions(10, "tp")
//partition_temporal_distribution(3119, "tw_lo.txt", "tp")

//#####################################################################################
// geoMetric Distance
//#####################################################################################
def deg2rad(deg:Double): Double = {return (deg * Math.PI / 180.0)}

def rad2deg(rad: Double ): Double = {return (rad * 180 / Math.PI)}

def geoDistance(lat1:Double, lon1:Double , lat2:Double, lon2:Double): Double = {
if(lat1 == lat2 && lon1 == lon2) return 0.0
var theta = lon1 - lon2
var geoDist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
geoDist = Math.acos(geoDist)
geoDist = rad2deg(geoDist)
geoDist = geoDist * 60 * 1.1515
return (geoDist * 1.609344)
}

def geoDistance_points(gp1:(Double,Double), gp2:(Double, Double)): Double = {
return geoDistance(gp1._1, gp1._2, gp2._1, gp2._2)
}

//######################################################################################
// Get Frequent locations for each user with less than X km threshold
//######################################################################################
case class Cons[+A] (val value: A, val next: Cons[A])

def get_freq_locations(locations:List[(Double, Double)]): List[(Double, Double)] = {
var freqLocations = Cons(locations(0), null)
for(i <- 1 to locations.size -1){
val l = locations(i)
var found = false
var current = freqLocations
while(current != null && !found){
 val fl = current.value
 if(geoDistance_points(l, fl) < 5){
  found = true
}
 current = current.next
}
if(!found){
 freqLocations = Cons(l, freqLocations)
}}
var result = List(freqLocations.value)
var next = freqLocations.next
while(next != null){
 result = result :+  next.value
 next = next.next
}
return result
}

//####################################################################################
// Extract Frequent locations for each user with less than X km threshold
//####################################################################################

case class Cons[+A] (val value: A, val next: Cons[A])

def get_frq_loc(locations:List[(Double, Double)], threshold: Integer): List[(Double, Double)] = {
var freqLocations = Cons(locations(0), null)
for(i <- 1 to locations.size -1){
val l = locations(i)
var found = false
var current = freqLocations
while(current != null && !found){
 val fl = current.value
 if(geoDistance_points(l, fl) < threshold){
  found = true
} 
 current = current.next
}
if(!found){
 freqLocations = Cons(l, freqLocations)
}}
var result = List(freqLocations.value)
var next = freqLocations.next
while(next != null){
 result = result :+  next.value
 next = next.next
}
return result
}

//####################################################################################
// Average Location Distance
//####################################################################################
def get_avg_dist(locations:List[(Double, Double)]): Double = {
if(locations.size == 1) return 0
var totalDist = 0.0
var count = 0
for(i <- 0 to locations.size -2){
for(j <- 1 to locations.size - 1){
count = count + 1
totalDist = totalDist + geoDistance_points(locations(i), locations(j))
}}
return totalDist/count
}

//######################################################################################
// geometric_median
//######################################################################################
def geometric_median(neighbors:List[(Double, Double)]): (Double,Double) = {
if(neighbors.size < 3) return (neighbors(0))
var distArray = Array.fill[Double](neighbors.size, neighbors.size)(-1)
var minDist = java.lang.Double.MAX_VALUE
var result = (-1.0, -1.0)
for(i <- 0 to neighbors.size - 1){
 var dist = 0.0
 val n1 = neighbors(i)
 for(j <- 0 to neighbors.size - 1){
   if(i != j){
    if(distArray(j)(i) != -1.0){
      dist += distArray(j)(i)
    }else{
      val n2 = neighbors(j)
      val newDist = geoDistance_points(n1, n2)
      distArray(i)(j) = newDist
      dist += newDist
    }}}
 if(dist < minDist) {
   minDist = dist
   result = n1
 }}
return result }

//######################################################################################
// geometric_median with text
//######################################################################################
def geoMetric_median_with_text(neighbors:List[((Double, Double), String)]): ((Double,Double),String) = {
if(neighbors.size == 1) return (neighbors(0))
if(neighbors.size == 2) return (neighbors(0)._1, neighbors(0)._2 + " " + neighbors(1)._2)
var distArray = Array.fill[Double](neighbors.size, neighbors.size)(-1)
var minDist = java.lang.Double.MAX_VALUE
var result = (-1.0, -1.0)
var text = ""
for(i <- 0 to neighbors.size - 1){
text += neighbors(i)._2 + " "
var dist = 0.0
val n1 = neighbors(i)
for(j <- 0 to neighbors.size - 1){
  if(i != j){
   if(distArray(j)(i) != -1.0){
    dist += distArray(j)(i)
   }else{
    val n2 = neighbors(j)
    val newDist = geoDistance_points(n1._1, n2._1)
    distArray(i)(j) = newDist
    dist += newDist
   }}}
if(dist < minDist) {
minDist = dist
result = n1._1
}}
return (result,text) }

//######################################################################################
// Extract User_Median_Location_Partition for Social Network Analysis
//######################################################################################
def extract_UMLP(st: String, tp: String, output: String, min_count: Int) ={
val stats = sc.textFile(st).map(_.split(",",7)).map(x => (x(1),(x(4).toDouble, x(3).toDouble)))
val valid_users = stats.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > min_count)
val UML = stats.join(valid_users).map({case(u,(l,x)) => (u,l)}).groupByKey().map({case(u,ls) => (u, geometric_median(ls.toList))})
val partitions = sc.textFile(tp).filter(x => !x.startsWith("#")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s")).filter(x => !x._2.contains("-")).map(x => (x._2, x._1))
val UMLP = UML.join(partitions)
UMLP.map({case(u,((lat, lon),p)) => u + "," + lat + "," + lon + "," + p}).saveAsTextFile(output)  
}

//######################################################################################
// Extract User_Median_Location_Partition for Social Network + Time Analysis
// Day 0, 1, 2 = (0-7, 8-18, 19-23)
//######################################################################################
def extract_UDTMLP(st: String, tp: String, output: String, min_count: Int) ={
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val UDL = sc.textFile(st).map(_.split(",",7)).map(x => (x(1),(dateparser.parse(x(2)), (x(4).toDouble, x(3).toDouble))))
val UDTL = UDL.map({case(u,(d,l)) => (u, (d.getDay, d.getHours, l))}).map({case(u,(d,t,l)) => (u, (if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2, l))})
//val UDTL = UDL.map({case(u,(d,l)) => (u, (d.getDay, d.getHours, l))}).map({case(u,(d,t,l)) => (u, (if(d % 6 > 0) 1 else 0, t , l))})
val valid_users = UDTL.map(x => (x._1, 1)).reduceByKey(_+_).filter(_._2 > min_count)
val UTML = UDTL.join(valid_users).map({case(u,((d, t, l),x)) => ((u, d, t), l)}).groupByKey().map({case((u, d, t),ls) => (u,(d, t, geometric_median(ls.toList)))})
val partitions = sc.textFile(tp).filter(x => !x.startsWith("#")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s")).filter(x => !x._2.contains("-")).map(x => (x._2, x._1))
val UTMLP = UTML.join(partitions)
UTMLP.map({case(u, ((d, t, (lat, lon)), p)) => u + "," + d + "," + t + "," + lat + "," + lon + "," + p}).saveAsTextFile(output)
}

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

//######################################################################################
// Extract User_Topic_Day_Time_Median_Location_Partition for Social Network + Topic + Time Analysis
// Topics are extracted before using a topic modeling algorithm like LDA
// Topics are in a file containing "Statusid	TopicID" named as sid_topic.txt
//######################################################################################
def extract_UTDTMLP(stats: String, topics: String , partitions: String, output: String, min_count: Int) ={
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val SUDL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(0),(x(1), dateparser.parse(x(2)), (x(4).toDouble, x(3).toDouble))))
val SUDTL = UDL.map({case(s, (u, d, l)) => (s, (u, d.getDay, d.getHours, l))}).map({case(s, (u, d, t, l)) => (s, (u, if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2, l))})
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
