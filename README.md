# go-transfer
数据转发，从 kafka 拉取数据发送到 ElasticSearch
可通过配置 etcd 的 key 来修改配置，实现实时更新

## etcd 配置
可以设置多个 topic 对应的 ES 信息
```
key :"server-{server_name}-conf"
vaule : '[
    {
        "index": "user".
        "type": "_doc",
        "topic": "web_log"
    }
]'

```

## ES
sender_nums：可以配置发送消息的 goroutine 数量
enabled_es_offset： 可配置保存消费者的 offset， 保证程序意外中断后数据不丢失
