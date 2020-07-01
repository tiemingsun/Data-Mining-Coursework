from graphframes import GraphFrame
from pyspark import SparkConf,SparkContext,SQLContext
from pyspark.sql import SparkSession
import os
import time
from graphframes.examples import Graphs

start_time = time.time()

os.environ["PYSPAK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

conf = SparkConf().setMaster("local")\
        .setAppName("task0")\
        .set("spark.executor.memory", "4g")\
        .set("spark.driver.memory", "4g")

spark_context = SparkContext(conf=conf)
spark = SparkSession(spark_context)
spark_context.setLogLevel("ERROR")

vertices = spark.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36)]
        # ("c", "Charlie", 30),
        # ("d", "David", 29),
        # ("e", "Esther", 32),
        # ("f", "Fanny", 36),
        # ("g", "Gabby", 60)]
        , ["id", "name", "age"])

edges = spark.createDataFrame([
        ("a", "b"),
        ("b", "a")
        # ("c", "b", "follow"),
        # ("f", "c", "follow"),
        # ("e", "f", "follow"),
        # ("e", "d", "friend"),
        # ("d", "a", "friend"),
        # ("a", "e", "friend")
    ], ["src", "dst"])

g = GraphFrame(vertices, edges)
# print("=> Here!",g)
# g.vertices.show()
# g.edges.show()
# g.vertices.groupBy().min("age").show()
# g.degrees.show()
print("I am here")
result = g.labelPropagation(maxIter=5)
# result.show()
result.select("id", "label").show()
print("I am done")
# motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
# motifs.show()
# filtered = motifs.filter("b.age > 30 or a.age > 30")
# g.outDegrees.show()

# filteredPaths = g.bfs(
#   fromExpr = "name = 'Esther'",
#   toExpr = "age < 32",
#   edgeFilter = "relationship != 'friend'",
#   maxPathLength = 3)

# filteredPaths.show()
print("Duration: ", time.time() - start_time)