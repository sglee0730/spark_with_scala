## Flatmap()

flatMap은 모나드 이해라든지 함수형에서 매우 중요

<b>map</b>

데이터를 각 row마다 읽음
```
ex) The quick fox => (THE,QUICK,FOX)
```

결과값을 타입으로 감싸서 반환
``` 
ex) List(1,2,None,3,None).map() => List(1,2,None,3,None)
```

<b>flatmap</b>

데이터의 row를 모두 합쳐서 읽음

```
ex) The quick fox => (T,H,E,Q,U,I,C,K,F,O,X)
```

결과값의 타입을 벗겨서 반환 
```
ex) List(1,2,None,3,None).flatmap() => List(1,2,3) 
```

## Input

```
book.txt
```

## Output

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
intimately: 1
desirable: 1
listening: 1
secure: 1
edits: 1
.
.
.
the: 1292
your: 1420
to: 1828
you: 1878

Process finished with exit code 0
```