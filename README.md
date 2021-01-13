# go-transfer
数据转发，从 kafka 拉取数据发送到 ElasticSearch

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
可以配置多个 goroutine 发送消息
保存消费者的 offset 保证程序意外中断后数据不丢失
