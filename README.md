#MergeDb#
    用于对于不同源有相同表结构的库表进行实时合表操作便于纵向分析，基于mysql binlog进行主从同步异构。

# 环境要求:
    python 2.7 版本
    mysql 5.5以上

# 安装库:
    pip install pymysql
    pip install mysql-replication
  
# 使用方法：

1. **授权主从用户**

    mysql 执行 grant select,replication slave,replication client on *.* to 'mergedb'@"localhost" identified by 'mergedb';FLUSH PRIVILEGES;
    
2. **异构端授权增删改查权限用户，以tidb为例**。
  
3. **配置文件设置**
      ``` php
      [mysql]
      user = mergedb
      passwd = mergedb
      host = localhost
      port = 3306
      charset = UTF8
      #socket 根据实际情况而定
      unix_socket = /opt/lampp/var/mysql/mysql.sock
      #server_id 根据实际情况而定
      server_id = 1
      #tidb 
      [tidb]
      user = meregdb
      passwd = meregdb
      host = your ip
      port = your port
      db = your db
      charset = UTF8
      #异构字段可以再次添加多个
       [addfield]
      clientid = 2
      #多个schema route 可以用逗号隔开，一一对应
      [route-rules]
      only_schema = chain
      route_schema = scm_merge_db
      ```
      
4. **注意事项**

      log_pos，log_file 第一次记录需要手动填进去，之后会自动记录更新
    
5. **测试运行**

      python row_merge.py
      
6. **生产环境**

      sh service start
      
7. **DTL**
      DTL用于从mysql 全量异构导入其他数据库
      导入规则根据自己的具体情况进行定义。本程序以tidb 为例，根据自己需求进行定义。
      
## 参考文献
    [文献1] (https://github.com/bjbean/mysqlbinlog-analysis "1")


  
