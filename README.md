# DisAlg DB-CDC

本项目为运行基于 CDC 的 Jepsen 测试的执行框架。

框架代码源于 Jepsen 对 PostgreSQL 执行的测试 [stolon](https://github.com/jepsen-io/jepsen/tree/main/stolon)。

## 1. 使用方法

本框架以支持 PostgreSQL 为例，后续将兼容 MySQL、Oracle 等数据库。
### 1.1 安装依赖环境
参考 https://github.com/jepsen-io/jepsen

### 1.2 配置数据库源
首先，修改数据库配置源，使得框架可以正常连接数据库。

本框架提供了一个模板文件，位于`resources/db-config-template.edn`，使用时重命名为`db-config.edn` 并填上数据源链接参数即可，基本是 JDBC 的那一套。

用户也可以增加自己的数据源。可参考 [db-spec-hash-map](https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.3.862/doc/getting-started#the-db-spec-hash-map)

### 1.3 运行测试
执行 `run.sh` 即可。如对参数有疑问，可以在命令行执行
``` bash
lein run test --help
```

### 1.4 添加对新数据库的支持
首先，需要在 `dbcdc.clj` 中搜索 `"--database DATABASE"`，修改所涉及的行，在 validate 添加修改的可支持选项。

之后，修改 `dbcdc/client.clj` 文件，在 `case` 语句中，完成对新增数据库的选项。
如果新增数据库是兼容 PostgreSQL 标准的，则可以直接复用函数。
否则，需要在 `dbcdc/impls` 中新建一个文件，并在 `client.clj` 中引用，完成各个函数（类似于用 if else-if 的方式来支持函数的重载）。
需要完成的函数有：
- `isolation-mapping`
- `set-transaction-isolation!`
- `create-table`
- `read`
- `write`

同时，在必要时，还需要自己进行异常的捕获与处理

## 2. 例子：增加对 TiDB 支持
下面以 TiDB 为例介绍本框架如何支持 TiDB。

### 2.1 配置数据源
首先，修改`resources/db-config.edn`，配置文件，增加 `:tidb` 键：
```clojure
:tidb       {:dbtype    "mysql"
            :host      "localhost"
            :port      4000
            :user      "root"
            :dbname    "test"}
```
由于 TiDB 兼容 MySQL 的语句和协议，因此这里的 `:dbtype` 设置为 `"mysql"` 即可。

`:tidb` 键将会贯穿本节的使用，后续将多次看到 `(:database test)` 这样的结构，就是从 Jepsen 框架中的 test 参数中取出使用的是那个数据库。

### 2.2 修改源代码
在 `dbcdc.clj` 中，加上 `:tidb` 选项：
``` clojure
[nil "--database DATABASE" "Which database should we test?"
:parse-fn keyword
:default :postgresql
:validate [#{:postgresql, :tidb}
            "Should be one of postgresql, tidb"]]
```
这样用户可以在命令行里通过 `--database tidb` 来指定这是一次对 TiDB 的测试。

然后，在 `client.clj` 中完成对 TiDB 的支持。从上到下，依次是：
- `open`：获得一个到数据库的连接。如果数据源配置无误，可以不用修改。当然，TiDB 有乐观事务与悲观事务两种模式，可以在开启链接的时候设置一下。当然这是后话。
- `isolation-mapping`：这一步的目的是通过一个映射，将待测隔离级别转换为解释成具体数据库的哪一个级别，如：
    - 快照隔离 `:snapshot-isolation` 在 PostgreSQL 中实际上对应可重复读 `:repeatable-read`。因此如果要测试快照隔离（其实也是本项目的目的），使用 JDBC 设置事务隔离级别的时候应该设置为可重复读。
    - 同样的，在 MySQL 中，快照隔离也是用可重复读表示。因此这里 TiDB 也得做同样的映射。
- `set-transaction-isolation!`：类似的，为当前链接`conn` 设置默认的隔离级别。由于类似的问题，同样的也是要做特别处理。
- `create-table`：创建一个数据表。这里也需要做单独处理，因此此处创建表的过程中，包括 1. 删旧表 2. 创新表 两个步骤，且有着 整型`int` 与字符`varchar`两种类型的变量，各个数据库的处理可能有点不大一样。
- `read/read-varchar`：
- `write/write-vachar`：


### 测试并修改
对这 6 组函数，我们先将 `impls/postgresql.clj` 的内容复制到 `impls/mysql.clj`，
然后把 client.clj 中 `:tidb` 的分支调用`mysql/<function>`，根据报错来修改。

首先运行脚本：

```bash
./run.sh tidb
```

一上来就遇上报错：
```log
WARN [2023-03-25 17:30:20,471] main - jepsen.core Test crashed!
java.sql.SQLException: No suitable driver found for jdbc:mysql://localhost:4000/test
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:702)
        at java.sql/java.sql.DriverManager.getConnection(DriverManager.java:189)
```
提示缺少合适的驱动。此时我们应该修改 `project.clj` 添加对应的驱动。
```clojure
[mysql/mysql-connector-java "8.0.27"]
```
运行一次测试了！不过 crash 了！

运行过程有许多错误，也就是说截至目前为止我们的测试只跑通了流程，没跑通逻辑。
下一步，跑通逻辑！

首先看报错：
``` log
java.sql.SQLSyntaxErrorException: You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 40 near "int not null primary key, val int not null)" 
        at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:120)
```
这就牵扯到 PostgreSQL 和 MySQL 的一个区别，MySQL 中是不建议把 `key` 做列的关键字的，这里改成 `k` 就行。

然后是第二个报错：
``` log
2023-03-27 15:45:56,226{GMT}	WARN	[jepsen worker 30] jepsen.generator.interpreter: Process 30 crashed
java.sql.SQLSyntaxErrorException: You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 19 near "AS t (k, v) VALUES (9, 2) ON CONFLICT (key) DO UPDATE SET v = 2 WHERE t.k = 9" 
	at com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:120)
	at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:122)
	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:953)
	at com.mysql.cj.jdbc.ClientPreparedStatement.execute(ClientPreparedStatement.java:371)
	at next.jdbc.result_set$stmt__GT_result_set.invokeStatic(result_set.clj:559)
```
这里也是语法的差异。`ON CONFLICT ... DO UPDATE` 算是 PostgreSQL 提供的语法糖，
我们需要替换成 MySQL 中的类似语句`ON DUPLICATE KEY UPDATE`。
同时，在 `client.clj` 中加入异常处理的代码。
```clojure
(defmacro with-errors
  "Takes an operation and a body; evals body, turning known errors into :fail
  or :info ops."
  [op & body]
  `(try ~@body
        (catch clojure.lang.ExceptionInfo e#
          (warn e# "Caught ex-info")
          (assoc ~op :type :info, :error [:ex-info (.getMessage e#)]))
        ...
        (catch com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException e#
          (condp re-find (.getMessage e#)
            #"Deadlock found when trying to get lock;"
            (assoc ~op :type :fail, :error [:deadlock (.getMessage e#)])

            (throw e#)))))
```
至此，一个完整的针对 TiDB 的测试就初步完成了。

## License

Copyright © 2023 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is avai
lable at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
