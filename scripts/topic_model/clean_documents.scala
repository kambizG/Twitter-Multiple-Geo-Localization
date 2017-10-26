
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

val sw = sc.textFile("/home/kambiz/data/tw_data_all_clean/tw_loi/scripts/topic_model/longstoplist.txt").collect
val stats = sc.textFile("stats.txt").map(_.split(",",7)).map(x => (x(0), x(6)))
val cleanStats = stats.map(x => (x._1, cleanRemoveStopWords(x._2, sw, 2, 15)))
cleanStats.filter(_._2.split("\\s").size > 3).map(x => x._1 + "," + x._2).saveAsTextFile("stats_clean")
exit
