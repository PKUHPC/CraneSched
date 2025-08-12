#!/bin/bash

# 直接内存分配脚本
# 目标：立即分配200MB内存

echo "开始直接分配200MB内存..."

# 方法：直接生成200MB的大字符串变量
echo "正在分配内存..."
start_time=$(date +%s)

# 直接分配200MB内存（209,715,200字节 = 200MB）
MEMORY_BLOCK=$(head -c 209715200 /dev/zero | tr '\0' 'M')

end_time=$(date +%s)
total_time=$((end_time - start_time))

echo "内存分配完成！已分配200MB内存 (用时: ${total_time}秒)"
echo "内存块大小: ${#MEMORY_BLOCK} 字节 ($(( ${#MEMORY_BLOCK} / 1024 / 1024 ))MB)"

# 显示当前进程的内存使用情况
echo "当前进程内存使用情况："
ps -o pid,ppid,cmd,%mem,vsz,rss -p $$

echo "按Enter键释放内存并退出..."
read -r

# 清理内存
unset MEMORY_BLOCK
echo "内存已释放"
