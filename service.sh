#!/bin/bash
#coding=utf-8
#auth: haochenxiao


#目录
dpath=/opt/dbbackup/tidb/conf

#启动binlog程序

function start()
{
    cd $dpath
    setsid python row_merge.py >>./binlog.log 2>&1 &
    sleep 1
    echo "start tidb_syncer_merge_db"
}

function stop()
{
ps -elf |grep row_merge.py|grep -v grep |awk '{print $4}'|xargs kill -9 
}

function status()
{
result=`ps -elf |grep row_merge.py|grep -v grep`
if [ "$result" = "" ];then
    echo "tidb_syncer_merge_db has been stopped!"
else
    echo "tidb_syncer_merge_db is started"
fi
}

function restart()
{
stop
start
}

case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;

  restart)
        restart
        ;;

  status)
        status
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart|status}"
        exit 2
esac
