df = spark.read.option('inferSchema', 'true').option('header', 'true').csv('covid-data.csv')
from pyspark.sql import functions as F
from pyspark.sql import Window

-- задание 1
-- Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)

df_1 = df.select('iso_code', 'location', F.col('total_cases_per_million')/1000).where((F.col('date')=='2020-03-31') & (~F.col('iso_code').like('OWID_%'))).sort(F.col('total_cases_per_million').desc())

df_1.withColumnRenamed("(total_cases_per_million / 1000)","процент переболевших").withColumnRenamed("location","страна").show(15)

+--------+-------------+--------------------+
|iso_code|       страна|процент переболевших|
+--------+-------------+--------------------+
|     VAT|      Vatican|            7.416564|
|     SMR|   San Marino|            6.953857|
|     AND|      Andorra|            4.866369|
|     LUX|   Luxembourg|  3.4793670000000003|
|     ISL|      Iceland|            3.326007|
|     ESP|        Spain|            2.051619|
|     CHE|  Switzerland|            1.918629|
|     LIE|Liechtenstein|            1.783045|
|     ITA|        Italy|            1.749732|
|     MCO|       Monaco|            1.325043|
|     AUT|      Austria|            1.130307|
|     BEL|      Belgium|             1.10228|
|     DEU|      Germany|            0.857062|
|     NOR|       Norway|            0.856077|
|     FRA|       France|            0.767128|
+--------+-------------+--------------------+



-- задание 2
--Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
--(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)

df_sort = df.select('date','location','new_cases').where((F.col("date")>='2021-03-25') & (F.col("date")<'2021-04-01') & (~F.col('iso_code').like('OWID_%')))
df_agg = df_sort.groupBy('location','date').sum('new_cases')

w = Window.partitionBy("location").orderBy(F.desc("sum(new_cases)"))
x = df_agg.select("date", "location", "sum(new_cases)",F.row_number().over(w).alias("rn"))
df_2 = x.select("date", "location", "sum(new_cases)").where("rn = 1")

df_2.withColumnRenamed("date","число").withColumnRenamed("location","страна").withColumnRenamed("sum(new_cases)","кол-во новых случаев").sort(F.col("кол-во новых случаев").desc()).show(10)

+----------+-------------+--------------------+
|     число|       страна|кол-во новых случаев|
+----------+-------------+--------------------+
|2021-03-25|       Brazil|            100158.0|
|2021-03-26|United States|             77321.0|
|2021-03-31|        India|             72330.0|
|2021-03-31|       France|             59054.0|
|2021-03-31|       Turkey|             39302.0|
|2021-03-26|       Poland|             35145.0|
|2021-03-31|      Germany|             25014.0|
|2021-03-26|        Italy|             24076.0|
|2021-03-25|         Peru|             19206.0|
|2021-03-26|      Ukraine|             18226.0|
+----------+-------------+--------------------+

--задание 3
--Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: -387) 
--(в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)

df_sort = df.select('date','location','new_cases').where((F.col("date")>='2021-03-25') & (F.col("date")<'2021-04-01') & (F.col("location")=='Russia'))
w = Window.partitionBy("location").orderBy(F.desc("date"))
df_lag = df_sort.select( "date", "location", "new_cases",F.lag("new_cases").over(w).alias("prev_cases"))
df_3 = df_lag.select("date", "new_cases", "prev_cases", F.col('new_cases')-F.col('prev_cases'))

df_3.withColumnRenamed("date","число").withColumnRenamed("prev_cases","кол-во новых случаев вчера").withColumnRenamed("new_cases","кол-во новых случаев сегодня").withColumnRenamed("(new_cases - prev_cases)","дельта").show()

+----------+----------------------------+--------------------------+------+
|     число|кол-во новых случаев сегодня|кол-во новых случаев вчера|дельта|
+----------+----------------------------+--------------------------+------+
|2021-03-31|                      8156.0|                      NULL|  NULL|
|2021-03-30|                      8162.0|                    8156.0|   6.0|
|2021-03-29|                      8589.0|                    8162.0| 427.0|
|2021-03-28|                      8979.0|                    8589.0| 390.0|
|2021-03-27|                      8783.0|                    8979.0|-196.0|
|2021-03-26|                      9073.0|                    8783.0| 290.0|
|2021-03-25|                      9128.0|                    9073.0|  55.0|
+----------+----------------------------+--------------------------+------+