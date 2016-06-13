import findspark
import os
import sys
findspark.init()
spark_home = findspark.find()

#spark_home = os.environ.get('SPARK_HOME', None)
sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))

# Adding the library to mysql connector
packages = "mysql:mysql-connector-java:5.1.37"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages {0} pyspark-shell".format(
    packages
)

# Initialize PySpark to predefine the SparkContext variable 'sc'
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))
