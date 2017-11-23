var max_comm_size = sc.textFile("max_comm_size.txt").first.toInt
sc.textFile("tp").map(_.split("\t")).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x).map(x => x._2 + "," + x._1).saveAsTextFile("partitions_"+ max_comm_size)	
System.exit(0)