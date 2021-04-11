## 1. Spark 기본기
Spark Programming Guide
https://spark.apache.org/docs/2.1.1/programming-guide.html#resilient-distributed-datasets-rdds

### 0) 기본 개념
처음에는 Spark 는 RDD 가 메인이었음.

### RDD interface
Resilient 회복력 있는, 물질이 탄력있는
* Resilient Distributed Data sets (DataSet 이랑 구별 필요 ㅋㅋㅋ)
(DataSet 은 higher API 인데, 이 RDD 로 이루어지긴 함)
* bascically RDD is a bunch of rows
  Because it's divided into rows,
  those rows can be distributed out to different computers.
  Divided those rows up amongst multiple computers
  => that's why distributed
* 왜 Resilient 인가? -> spark 는 여러 computer 에서 분산 처리중에, 문제가 발생하면. 
그 부분을 체크해서, 다른 노드를 띄워서 일을 재진행시킴. 즉, 일이 완수됨을 보장함.

RDD 를 만들기 전엔 sparkContext 를 만들어야 한다.
Driver Program 은 제일 처음 SparkContext 를 만들고, 이 SparkContext 가 사용자의 요청을 해석해서, RDD 를 만들고 잘 분배되는 걸 책임진다.
RDD 를 사용하기 때문에, 클러스터에 데이터가 어떻게 나눠져 진행 되는지, 어떤 Node 혹은 하드웨어 실패에 대응하는 코드라던지, 이런걸 신경 쓸 필요 없다.
SparkContext 안의 RDD 가 이를 알아서 해준다.

// 
- map
- flatmap
- filter
- distinct
- sample
- union, intersection, subtract, cartesian


SparkShell 을 만들어도 자동으로 이 SparkContext를 만든다.

RDD 를 만드려면 기본적으로

val num = parallelize(List(1,2,3,4)) 이런 식으로 생성할 수 있다.
다만 어차피 이런건 말도 안된다. (리얼 환경에서 이런 식으로 쓸 리가 ㅇ벗다.)

sc.textFile("file:///c:/users/frank/gobs-o-text/txt")

꼭 파일 뿐만이 아니라, Hive Context 도 해석할 수 있다.

당연히 빅데이터를 처리할 수 있어야 하니
HDFS, Amazon S3, 등등
많은 distributed data stores 에서 데이터를 읽어들일 수 있는 extensions 가 있다. 왜냐면,, Spark는 BigData 를 위해 존재하니까.


RDD 연산에는, 함수가 들어가는 경우가 많다.

RDD 의 Action 의 기본은, driver script 에 answer 를 전달하는 것이다.
driver program 에선 어떤 일도 일어나지 않는다. ACTION 을 호출하기 전에는.
action 이 호출되면,  Directed Acyclic Graph(방향성 비사이클 그래프) 를 만들고 Cluster 의 여러 노드에 최적화된 필요한 연산을 호출한다.



### 1) RatingsCounter

Local에서 Spark 프로그램을 돌리는 가장 기본적인 방법을 설명 해준다.
```
val sc = new SparkContext("local[*]", "RatingsCounter")
```
에서, SparkContext 를 만드는 것을 볼 수 있다.
 
```
val lines = sc.textFile("data/ml-100k/u.data")`
```

파일을 위와 같이 읽어온다. 이후는, 기본적인 mapping 관련 내용이다.
여기서 중요한 것은,

```
def main(args: Array[String]) {....}
``` 
이 구조가, 기본적으로 모든 driver script 가 아파치 스파크에서 작업을 수행할 수 있는 구조라는 것이다.

또한 위의 예시에서, textFile 을 읽고 map 하는 부분 까지를 stage1
countByValue() 를 호출하는 부분을 stage2 라고 할 수 있다.

job 은 데이터가 재생성(?) 되는 기준에 따라 stage 여러개로 나뉘게 된다.

---

### 2) FiendsByAge

Key, Value 형태로 데이터를 변경한 후에, 어떤 방식으로 데이터를 다룰 수 있는지 알려주는 예시.
사실 Key, Value 라지만 Pair 형태의 Tuple 일 뿐이다.


Key, Value 상태의 데이터를 다룰 수 있는 함수에는 다음과 같은 것들이 있다.

- reduceByKey: rdd.reduceByKey((x,y) => x + y)
- groupByKey(): Group Values with the same Key
- sortByKey: Sort RDD by key values
- keys(), values(): Create an RDD of just the keys, or just the values.

여기에서 추가적으로, join, rightOuterJoin... cogroup, subtractByKey 등을 쓸 수 있다.


### 3) Filtering RDD's: MinTemperatures

// input data snippet
stationId/ 
ITE00100554, 18000101, TMAX, -75,,,E, <- -75 == -7.5 (* 10 돼있음
)  

```
val minTempsByStation = stationTemps.reduceByKey((x,y) => min(x,y))
```

### 4) MAP: WordCount
### 5) MAP: WordCountBetter (with Regex)
이번 기회에 정규 표현식에 대해서 좀 더 배울 수 있었다.
\W <- 문자 제외
\w <- 문자

### 6) MAP: WordCountBetterSorted
데이터가 무지 클 때, sorting 까지 해서 주면 정말 좋겠지?
```
    // Load each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

....

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y ) // countByValue 를 직접 구현.
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
```

### 7) Practice : TotalAmountSpentFkane
여기서 매우 중요한 것을 깨달을 수 있다.

```
    val sc = new SparkContext("local[*]", "TotalAmountSpentFkane")
```

여기서 SparkContext 에 local[*] 이 있다면, local 모든 코어를 다 사용하겠다는 뜻이다.
이 상태에서,

```
    val userPaids = userIdsAndPrices.reduceByKey((x, y) => x + y).map(target => (target._2, target._1)).sortByKey()
```
이렇게 sorting 을 한다음, 바로 출력하면 순서가 뒤죽박죽이 된다. 즉, sorting 이 안된 것처럼 보인다.
왜 그럴까? 바로 스레드가 여러개로 돌았기 때문이다... 내 로컬 머신의 코어 갯수만큼 스레드를 생성해서 돌린다.
=> 여러 곳에서 sorting 한후 결과를 합친 것이다.

만약 local[*] 이 아니라, local 이었으면 제대로 sorting 된 값이 출력된다.
그 대표적인 예시가 바로 WordCountBetterSorted 다.


따라서  **TotalAmountSpentFkane** 에서는 반드시, collect() 작업을 수행 해야 한다.





