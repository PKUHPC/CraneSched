#!/bin/bash

set -x  # 开启调试

DIR="$( cd "$( dirname "$0" )/.." && pwd )"
echo DIR

kill_process() {
  pid=`ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}'`
  if [ ! -z "$pid" ]; then
    kill -9 $pid
    echo "killing process '$1' $pid"
  fi
}

need_compile=false
while getopts icha: flag; do
  case $flag in
    i)
      init
      ;;
    c)
      need_compile=true
      ;;
    a)
      test_args=$OPTARG
      ;;
    h)
      usage
      exit 0
      ;;
    ?)
      usage
      exit 0
      ;;
  esac
done

if [ "$need_compile" = true ]; then
  cd "$DIR/.."
  ## 执行编译
  if [ $? != 0 ]; then
    echo "compile failed"
    exit 1
  fi
fi

cd $DIR
python3 src/main.py $test_args --config_path=conf/conf.yaml --folder='testcases'

usage() {
  echo "script usage: $0 [-i] [-c] [-a args]"
  echo "  -i 初始化。第一次执行system test或proto定义有更新时，需指定。后续不需要加"
  echo "  -c 编译工程"
  echo '  -a 运行system test脚本所需参数，args要包含在""里，参考指定执行case1和case2, `./start.sh -a "--case=case1,case2"`'
}