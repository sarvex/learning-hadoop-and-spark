import pyspark
import random

if 'sc' not in globals():
    sc = pyspark.SparkContext()

NUM_SAMPLES = 1000

def sample(p):
    x,y = random.random(),random.random()
    return 1 if x**2 + y**2 < 1 else 0

count = sc.parallelize(range(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)

print ("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))