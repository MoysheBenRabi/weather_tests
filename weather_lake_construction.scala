/*
Это само по себе даже не будет работать - обвязка драйвера в zeppelin
Выполнялось 5 c половиной часов, но это делается однократно при наполнение
озера данных (обычно, делается однократно, но при наличии достаточных ресурсов
можно и на каждое обновление перетраивать ее с нуля. перестроение раз в день
объем не большой, история изменеий истроии не интересна, только сама погода) 
В продуктовом коде будет что-то вида
val weather_raw = spark.read
    .format("delta")
    .load("hdfs://s3.lan:9000/lake/ghcn/")
    .filter(col("TIMESTAMP") === "2019-01-01 00:00:00.0" && col("STATION") === "USC00477118" )
    .limit(1000)
(выполняется в среднем 12-19 секунд на тестовом кластере)
Применение Delta Lake позволяет значительно упростить сценарий инкрементного обновления,
переход к полному перестроению озера при обновление позволит отказаться от Delta Lake,
или других баз данных в пользу простых файлов parquet.
Повысить скорость можно заменив Delta Lake на субд (InMemory Key-value, например Redis, Apache Ignite)
*/

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode.Overwrite

// Можно было использовать infereSchema - но я и так знаю какая именно схема нужна.
// И не факт что spark выведет ее так, как мне надо.
val ds1SchemaTyped = new StructType()
    .add("STATION", "string")
    .add("DATE", "string")
    .add("ELEMENT", "string")
    .add("DATA", "int")
    .add("M-FLAG", "byte",true)
    .add("Q-FLAG", "byte",true)
    .add("S-FLAG", "byte", true)
    .add("TIME", "string", true)
  
val core = List("PRCP","TMIN","TMAX")  
  
// Биндинг weather сейчас не используется, сделано так потому, что иногда хотелось посмотреть: а что же внутри
val weather = spark.read
    .format("csv")
    .option("sep", ",")
    .option("encoding", "ASCII")
    .option("nullValue","NULL")
    .schema(ds1SchemaTyped)
    // Распаковывать зарание не стал - мало места. В одном из тестов попробовал сделать
    // persist - место на ноде кончилось раньше чем задача (весь набор данных без фильтра.)
    .load("hdfs://s3.lan:9000/data/ghcn/superghcnd_full_20191102.csv.gz")
    // Это тест - преобразовать потом все на большом кластере
    .filter($"DATE".substr(0,4) >= "2018")
    .filter($"Q-FLAG".isNull)
    .filter($"ELEMENT".isin(core:_*))
    .withColumn("TIMESTAMP",
        to_timestamp(concat(
            when($"DATE".isNotNull, $"DATE").otherwise(lit("00000000")),
            when($"TIME".isNotNull, regexp_replace($"TIME","^24","00")).otherwise(lit("0000"))),"yyyyMMddHHmm"))
    // Теоретически можно было просто выкинуть время и преобразовать DATE в дату (если очень постараться, то наверное даже
    // на уровне схемы но пока так.
    .drop($"DATE")
    .drop($"TIME")
    // Схема разбиения на разделы должна быть STATION,TIMESTAMP
    .groupBy($"STATION",$"TIMESTAMP").agg(collect_list(map($"ELEMENT",$"DATA")) as "DATA")
    .withColumn("DATA", to_json($"DATA"))
    // Обычно я с опаской отношусь к тому, что мажорную версию меньше единицы - но эти ребята создатели спарка, да и на
    // гитхабе не жалуются - так что Delta Lake, может быть попробую и вариант со структурированным  потоком, но вообще
    // это все еще медленные большие данные (обновляются раз в день), так что потоки не обязательны.
    .write
    .partitionBy("STATION","TIMESTAMP")
    .format("delta")
    .mode(Overwrite)
    .save("hdfs://s3.lan:9000/lake/ghcn2/")

z.show(weather)
