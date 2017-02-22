val textFile = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
textFile.flatMap(line=>line.split("\t")).distinct.count()
