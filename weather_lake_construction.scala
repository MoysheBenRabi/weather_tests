/*
Это само по себе не будет работать.
Фрагмент кода для блокнота. 
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
    .filter($"DATE".substr(0,4) >= "2018" && $"Q-FLAG".isNull)
    .withColumn("TIMESTAMP",
        to_timestamp(concat(
            when($"DATE".isNotNull, $"DATE").otherwise(lit("00000000")),
            when($"TIME".isNotNull, regexp_replace($"TIME","^24","00")).otherwise(lit("0000"))),"yyyyMMddHHmm"))
    // Теоретически можно было просто выкинуть время и преобразовать DATE в дату (если очень постараться, то наверное даже
    // на уровне схемы но пока так.
    .drop($"DATE")
    .drop($"TIME")
    .groupBy($"STATION",$"TIMESTAMP").agg(collect_list(map($"ELEMENT",$"DATA")) as "DATA")
// Зависит от постановки задачи.
// Вариант с pivot работает, но мне не нравится. 
//  .groupBy($"STATION",$"TIMESTAMP").pivot("ELEMENT").agg(
//        .when(size(collect_list($"DATA")) === 0, null).
//        .otherwise(collect_list($"DATA")) as "DATA")
    .withColumn("DATA", to_json($"DATA"))
    // Обычно я с опаской отношусь к тому, что мажорную версию меньше единицы - но эти ребята создатели спарка, да и на
    // гитхабе не жалуются - так что Delta Lake, может быть попробую и вариант со структурированным  потоком, но вообще
    // это все еще медленные большие данные (обновляются раз в день), так что потоки не обязательны.
    .write
    .format("delta")
    .mode(Overwrite)
    .save("hdfs://s3.lan:9000/lake/ghcn2/")

//z.show(weather)
