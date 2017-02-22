val textFile = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
textFile.distinct.count()
