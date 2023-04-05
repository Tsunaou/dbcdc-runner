#!/bin/bash
# 获取操作系统类型
os=$(uname -s)

# 根据操作系统类型设置 BASE 变量
if [[ "$os" == "Darwin" ]]; then
  CDC_DATA_HOME="/tmp/cdc_data"
else
  CDC_DATA_HOME="/data/home/tsunaouyang/tmp/cdc_data"
fi

echo "Start watching $CDC_DATA_HOME/tmp.log"
kill $(ps aux | grep 'tail -f' | grep 'cdc_data/tmp.log' | awk '{print $2}')
tail -f $CDC_DATA_HOME/tmp.log | awk '/RowChangeEvent/ {print $0; fflush() }' >> cdc.log
echo "Finish watching $CDC_DATA_HOME/tmp.log"