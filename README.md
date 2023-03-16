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
