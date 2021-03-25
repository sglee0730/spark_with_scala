## DataSets

<b>RDD</b>
<li>벙렬처리
<li>연산이 빠르다
<li>Immutable
<li>Transformation과 Action 함수로 나뉨


<b>Dataset</b>
<li>RDD 와 DataFrame 의 장점
<li>Type-safe
<li>Query로 사용 가능하며 개발을 단순화해줌
<li>컴파일 언어에서만 사용 가능 (sorry Python)


## Output

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Here is our inferred schema:
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- friends: integer (nullable = true)

Let's select the name column:
+--------+
|    name|
+--------+
|    Will|
|Jean-Luc|
|    Hugh|
|  Deanna|
|   Quark|
|  Weyoun|
|  Gowron|
|    Will|
|  Jadzia|
|    Hugh|
|     Odo|
|     Ben|
|   Keiko|
|Jean-Luc|
|    Hugh|
|     Rom|
|  Weyoun|
|     Odo|
|Jean-Luc|
|  Geordi|
+--------+
only showing top 20 rows

Filter out anyone over 21:
+---+-------+---+-------+
| id|   name|age|friends|
+---+-------+---+-------+
| 21|  Miles| 19|    268|
| 48|    Nog| 20|      1|
| 52|Beverly| 19|    269|
| 54|  Brunt| 19|      5|
| 60| Geordi| 20|    100|
| 73|  Brunt| 20|    384|
|106|Beverly| 18|    499|
|115|  Dukat| 18|    397|
|133|  Quark| 19|    265|
|136|   Will| 19|    335|
|225|   Elim| 19|    106|
|304|   Will| 19|    404|
|327| Julian| 20|     63|
|341|   Data| 18|    326|
|349| Kasidy| 20|    277|
|366|  Keiko| 19|    119|
|373|  Quark| 19|    272|
|377|Beverly| 18|    418|
|404| Kasidy| 18|     24|
|409|    Nog| 19|    267|
+---+-------+---+-------+
only showing top 20 rows

Group by age:
+---+-----+
|age|count|
+---+-----+
| 31|    8|
| 65|    5|
| 53|    7|
| 34|    6|
| 28|   10|
| 26|   17|
| 27|    8|
| 44|   12|
| 22|    7|
| 47|    9|
| 52|   11|
| 40|   17|
| 20|    5|
| 57|   12|
| 54|   13|
| 48|   10|
| 19|   11|
| 64|   12|
| 41|    9|
| 43|    7|
+---+-----+
only showing top 20 rows

Make everyone 10 years older:
+--------+----------+
|    name|(age + 10)|
+--------+----------+
|    Will|        43|
|Jean-Luc|        36|
|    Hugh|        65|
|  Deanna|        50|
|   Quark|        78|
|  Weyoun|        69|
|  Gowron|        47|
|    Will|        64|
|  Jadzia|        48|
|    Hugh|        37|
|     Odo|        63|
|     Ben|        67|
|   Keiko|        64|
|Jean-Luc|        66|
|    Hugh|        53|
|     Rom|        46|
|  Weyoun|        32|
|     Odo|        45|
|Jean-Luc|        55|
|  Geordi|        70|
+--------+----------+
only showing top 20 rows


Process finished with exit code 0

```