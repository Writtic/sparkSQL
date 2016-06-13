from pyspark import SparkContext
sc=SparkContext()
# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
import urllib
from datetime import timedelta, date

def load_day(filename, mydate):
    # Load a text file and convert each line to a Row.
    lines = sc.textFile(filename)
    parts = lines.map(lambda l: l.split(" ")).filter(lambda line: line[0]=="en").filter(lambda line: len(line)>3).cache()
    wiki = parts.map(lambda p: Row(project=p[0],  url=urllib.unquote(p[1]).lower(), num_requests=int(p[2]), content_size=int(p[3])))
    #wiki.count()
    # Infer the schema, and register the DataFrame as a table.
    schemaWiki = sqlContext.createDataFrame(wiki)
    schemaWiki.registerTempTable("wikistats")
    group_res = sqlContext.sql("SELECT '"+ mydate + "' as mydate, url, count(*) as cnt, sum(num_requests) as tot_visits FROM wikistats group by url")
    # Save to MySQL
    mysql_url="jdbc:mysql://localhost:3306/?user=yongjang&password=yongjang"
    group_res.write.jdbc(url=mysql_url, table="wikistats.wikistats_by_day_spark", mode="append")
    # Write to parquet file - if needed
    group_res.saveAsParquetFile("/ssd/wikistats_parquet_bydate/mydate=" + mydate)
if __name__ == "__main__":
    mount = "/data/wikistats/"

    d= date(2008, 1, 1)
    end_date = date(2008, 2, 1)
    delta = timedelta(days=1)
    while d < end_date:
        print d.strftime("%Y-%m-%d")
        filename=mount + "wikistats//dumps.wikimedia.org/other/pagecounts-raw/2008/2008-01/pagecounts-200801" + d.strftime("%d") + "-*.gz"
        print(filename)
        load_day(filename, d.strftime("%Y-%m-%d"))
        d += delta
