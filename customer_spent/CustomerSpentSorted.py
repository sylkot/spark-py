from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile(r"...\data\customer-orders.csv")
parsedLines = lines.map(parseLine)

amountTotal = parsedLines.reduceByKey(lambda x,y: x+y)
amountTotalSorted = amountTotal.map(lambda x: (x[1], x[0])).sortByKey()

results = amountTotalSorted.collect()

for result in results:
    print(result[1], "\t{:.2f}USD".format(result[0]))
