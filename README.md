1、连接应答处理器：acceptTcpHandler

2、命令请求处理器：readQueryFromClient

3、命令回复处理器：sendReplyToClient

主从复制的两次文件事件回调函数

1、初始化的时候执行一次：syncWithMaster

2、以后的主从同步都是由该函数处理：readSyncBulkPayload