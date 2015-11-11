import sys

from pyspark import SparkContext

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "WordCount")
    
	#Below lines can be executed thorugh the spark command line interface to get the value
	lines = sc.parallelize(["Machine Learning language", "i like NumPy"])
    result = lines.flatMap(lambda x: x.split(" ")).countByValue()
    for key, value in result.iteritems():
        print "%s %i" % (key, value)
