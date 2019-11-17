%spark
/*
Это само по себе даже не будет работать - обвязка драйвера в zeppelin
Вообще, было бы горзадо легче преобразовать все в python, но ради практики
сделано в spark
*/

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode


/*
ID            1-11   Character
LATITUDE     13-20   Real
LONGITUDE    22-30   Real
ELEVATION    32-37   Real
STATE        39-40   Character
NAME         42-71   Character
GSN FLAG     73-75   Character
HCN/CRN FLAG 77-79   Character
WMO ID       81-85   Character
*/

val stationSchemaTyped = new StructType()
    .add("ID", "string")
    .add("LATITUDE", "double")
    .add("LONGITUDE", "double")
    .add("ELEVATION", "double")
 
val stations = spark.read
    .format("csv")
    .option("sep", ";")
    .option("encoding", "ASCII")
    .option("nullValue","NULL")
    .schema(stationSchemaTyped)
    .load("hdfs://s3.lan:9000/data/ghcn/ghcnd-stations.txt")
    .withColumn("STATION",$"ID")
    .drop($"ELEVATION")
    .drop($"ID")
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .save("hdfs://s3.lan:9000/lake/ghcnd-stations.csv")
  
    
