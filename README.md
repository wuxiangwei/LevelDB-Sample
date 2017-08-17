
检查leveldb下面的问题：
https://github.com/google/leveldb/issues/227


```python
import leveldb

db = leveldb.LevelDB('testdb')
for k in db.RangeIter(include_value=False):
    print k
```
通过上述代码片段打印，testdb的key值，可以发现结果都一样：所有Key都被删掉了。





