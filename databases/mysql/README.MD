# 调试用 MySQL

使用本地 docker 启动的 MySQL 作为 workload 生成与调试的对象。

## MySQL 安装
安装 MySQL 的教程可以参考：[菜鸟教程：Docker 安装 MySQL
](https://www.runoob.com/docker/docker-install-mysql.html?_=1665737981407)

## 配置文件：
```clojure
{:mysql      {:dbtype    "mysql"
              :host      "127.0.0.1"
              :port      3306
              :dbname    "test" 
              :user      "root"
              :password  "123456"}}
```
