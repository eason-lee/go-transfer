# go-transfer
- 数据转发，从 kafka 拉取数据发送到 ElasticSearch
- 可配置数据过滤器，删除或修改数据
- 可配置数据批量发送
- 灵活配置数据的拉取和发送的 goroutine 数量
- 服务优雅停止

## 配置说明

### kafka

```shell
group: transfer
Conns: 2
Consumers: 2
Processors: 2
MinBytes: 1048576
MaxBytes: 10485760
Offset: first
```
#### group 
  kafka 的消费组的名称

#### Conns
  链接kafka的链接数，链接数依据cpu的核数，一般<= CPU的核数；

#### Consumers
  每个连接数打开的线程数，计算规则为Conns * Consumers，不建议超过分片总数，比如topic分片为30，Conns *Consumers <= 30

#### Processors
  处理数据的线程数量，依据CPU的核数，可以适当增加，建议配置：Conns * Consumers * 2 或 Conns * Consumers * 3，例如：60  或 90

#### MinBytes MaxBytes
  每次从kafka获取数据块的区间大小，默认为1M~10M，网络和IO较好的情况下，可以适当调高

#### Offset
  可选last和false，默认为last，表示从头从kafka开始读取数据


### Filters

```shell
- Action: drop_field
  Fields:
    - topic
    - index
    - beat
    - offset
- Action: update_field
  Field: message
  Target: data
```
#### - Action: drop_field
  移除字段标识：需要移除的字段，在下面列出即可

#### - Action: update_field
  转移字段标识：例如可以将message字段，重新定义为data字段


### elasticsearch

#### Index
  索引名称

#### max_chunk_bytes
  每次往ES提交的bulk大小，默认是1M，可依据ES的io情况，适当的调整

#### Compress
  数据压缩，压缩会减少传输的数据量，但会增加一定的处理性能，可选值true/false，默认为false
