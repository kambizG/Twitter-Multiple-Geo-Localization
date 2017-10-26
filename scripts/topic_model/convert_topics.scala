sc.textFile("LDA/model-final.theta").map(_.split("\\s").map(_.toDouble).zipWithIndex.maxBy(_._1)._2).saveAsTextFile("topics")
exit
