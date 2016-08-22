# mq_jstorm
Jstorm组合消息队列中间件开发Demo

Jstorm 开发的demo，spout 不断有RabbitMq产生数据，blot 接受数据经过翻译处理，继续发送到RabbitMq上，为了存储hbase和web端展现。

涉及点：
Jstorm 开发规范，topology、spout、bolt 的开发模式；
RabbitMq消息队列组件不断产生数据给spout，bolt操作 hbase 、oracle数据库，最简单的开发。
