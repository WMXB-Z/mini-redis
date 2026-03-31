1、构建项目

cd mini-redis

cmake -S . -B build

cmake --build build -j

2、单机启动所支持的持久化模型

1）无持久化模式（配置文件：none.conf）

启动服务器，服务器将在端口 6388 启动，无 AOF 和 RDB：

./build/mini_redis --config build/none.conf

2）每秒同步模式 （配置文件：everysec.conf）

启动服务器，服务器将在端口 6388 启动，AOF 每秒同步一次：

./build/mini_redis --config build/everysec.conf


3）⽴即同步模式 （配置文件：always.conf）

启动服务器，服务器将在端口 6388 启动，每个写操作立即同步到磁盘：

./build/mini_redis --config build/always.conf

3、多机启动所支持的主从复制模式

启动主节点（配置文件：master.conf）：

./build/mini_redis --config master.conf

启动从节点（配置文件：replica.conf）：

./build/mini_redis --config replica.conf
