#!/bin/bash

# ************************************************************************
#  Filename     corgi.sh
#  Revision     0.0.1
#  Date         2017/06/05
#  Author       mingh.yang
#  Description  The shell for creating project, executing data clean 
#               process, quering data and so forth
#  Version      0.0.1    
# ************************************************************************

CORGI_BIN_PATH=$(pwd)/$(basename $0)
CORGI_PATH=${CORGI_BIN_PATH%/*}

function logs() {

	LOG_FILE=$1/logs/corgi_$(date "+%Y-%m-%d").log
	FIFO_FILE=$1/logs/corgi.fifo
	[ ! -p $FIFO_FILE ] && mkfifo ${FIFO_FILE}			
	cat ${FIFO_FILE} | tee -a ${LOG_FILE} &
	exec 1> ${FIFO_FILE}
	exec 2>&1
}

help()
{
    echo "
NAME
     corgi.sh -- corgi 数据清洗功能脚本

SYNOPSIS

DESCRIPTION
     本脚本用于自动化清洗流程.

     可选参数如下:

     -p|--project   创建工程

     -r|--rum       执行channel洗数脚本

     -q|-query      查询数据  

EXIT STATUS
     0: 任务执行成功; 其他: 任务执行失败.

EXAMPLES
     命令:

           ./corgi.sh \\
           -p \\
           /path/to/project

     此命令将创建一个新的工程目录.
    "
}

function createProject {
	
	if [[ ! -d $1 ]];
	then
	  #创建工程目录
	  mkdir $1
	  #创建工程配置文件目录并拷贝配置文件模板
	  mkdir $1/conf
	  cp $CORGI_PATH/conf/*.template $1/conf/
	  #创建工程ETL脚本文件目录并拷贝配置ETL脚本文件模板,主要为chain脚本和channel脚本
	  mkdir $1/channel
	  cp $CORGI_PATH/channel/*.template $1/channel/
	  #创建工程的校验脚本文件目录并拷贝配置文件模板
	  mkdir $1/validation
	  cp $CORGI_PATH/validation/*.template $1/validation/
	  #创建工程日志文件目录
	  mkdir $1/logs
      
      echo "$(date "+%Y-%m-%d %H:%M:%S") 工程目录创建完成"
     
	else
	  echo "$(date "+%Y-%m-%d %H:%M:%S") 工程目录已经存在"
	fi
}

function executeProject {

	logs $1

	if [[ ! -d $1 ]];
	then
	  echo "$(date "+%Y-%m-%d %H:%M:%S") 无法找到指定的工程目录" && exit 1
	fi

	if [[ ! -f $1/conf/corgi-conf.xml ]];
	then 
	  echo "$(date "+%Y-%m-%d %H:%M:%S") 工程配置文件不存在，请检查工程" && exit 1
	fi

	if [[ ! -f $1/channel/main.cha.xml ]];
	then 
	  echo "$(date "+%Y-%m-%d %H:%M:%S") 主channel文件不存在，请检查工程" && exit 1
	fi

	spark-submit --driver-memory 2g  --conf spark.yarn.executor.memoryOverhead=50000 --class com.corgi.channel.Controller $CORGI_PATH/libs/corgi-0.0.1-SNAPSHOT.jar $1/conf/corgi-conf.xml $1/channel/main.cha.xml

}

function query {

	echo $1

	spark-submit --class com.corgi.query.Controller $CORGI_PATH/libs/corgi-0.0.1-SNAPSHOT.jar $1
}

#创建日志信息管道并将信息输入到日志文件中
logs $CORGI_PATH

# init parameter - get user define parameters
# options=$(getopt -o p:r:q: -l project:,run:,query: help -n '$(basename $0)' -- "$@")
# [ $? != 0 ] && echo "Failed to parse options." >&2 && exit 0

# eval set -- "$options"

[ $# -eq 0 ] && help && exit 1
while [ $# -gt 0 ]
do
	case $1 in
	  -p|-project)
	   createProject $2
	   shift 2
	   ;;
	  -r|-run)
	   executeProject $2
	   shift 2
	   ;;
	  -q|-query)
	   query $2
	   shift 2
	   ;;
	  --) 
		shift 1; 
		break 
		;;
	  *)
	    echo "$1 is not a option paramter."
	    help && exit 1
	    ;;
	esac
done   



