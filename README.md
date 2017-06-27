# corgi

Corgi 是基于Spark SQL和XML文件的轻量级的分布式ETL工具。用户可以不用编写任何代码，只需要编辑XML配置文件和SQL语句就能对HDFS或本地磁盘上的数据进行查询和清洗工作。

Corgi主要由三大部分组成，一部分是Channel模块，一部分是Validation模块，最后一部分是Query模块。Channel模块主要负责对数据的清洗，而Validation模块负责对数据的校验，Query模块则是用来对数据进行查询。所有的Corgi功能模块都在Corgi项目的管理下，每个Corgi项目都有自己独立的metastore元数据库和Spark-warehouse数据库，其中Spark-warehouse 数据库用来存储Channel模块产生的中间数据，当用户执行数据清洗过程中发现数据问题，可以用Query模块提供的功能来使用SQL对存储在Spark-warehouse数据库中的中间数据进行查询，而不用再在Spark-sql中写代码来注册中间数据和进行查询。

## Channel模块

Channel模块负责定义数据源，数据清洗流程和数据的下沉。Channel模块的功能配置文件由入口文件main.cha.xml 和 管道配置文件Channel-Name.cha.xml两个XML文件组成，它们都放在项目的channel文件夹下。

管道配置文件的名字用户可以进行自定义，比如 filter-tier.cha.xml, generateid-tier.cha.xml, 管道配置文件里面定义了从哪获取数据，如何用SQL查询和清洗数据以及把数据写到哪里。在管道配置文件中，用户需要定义Sink，指明数据从什么数据源中获取，然后把数据映射成Spark SQL 中的一张表；接下来定义Transaction，里面是SQL语句，用来对被映射成表的数据进行操作，类似关系型数据库；最后定义Sink部分，用来指明经过SQL语句查询和操作的数将存储在哪里。

![Image text](https://github.com/guludada/corgi/blob/master/images/channel.png)

而main.cha.xml 文件名字是固定的，它用来指定执行哪些管道配置文件，以及按什么顺序来执行。

![Image text](https://github.com/guludada/corgi/blob/master/images/channel-main.png)
 



## Validation模块

Validation模块主要负责对Channel模块产生的中间过程数据以及结果数据进行校验，该模块与Channel类似，也是由两个XML配置文件来实现功能，一个是入口文件main.valid.xml 和 数据验证配置文件Channel-Name.valid.xml，这两个配置文件都需要放在项目工程中的validation文件夹下。

数据验证配置文件的名字用户可以进行自定义，比如 filter-tier.valid.xml, generateid-tier.valid.xml, 数据验证配置文件里面定义了多种”validation”类型的Transaction，用来对Channel模块产生的中间过程数据或者结果数据进行校验，比如输入输出平衡的校验或者是否存在脏数据的校验等。

![Image text](https://github.com/guludada/corgi/blob/master/images/validation.png)
 
而main.valid.xml 文件名字是固定的，它用来指定执行哪些数据校验配置文件，以及按什么顺序来执行。

![Image text](https://github.com/guludada/corgi/blob/master/images/validation-main.png)
 

## Query模块
所有Channel执行清洗过程中产生的中间数据都会存储在其所在项目指定的Spark warehouse数据库中，这时用户可以使用Corgi提供的Query功能使用SQL语句对这些中间数据方便地进行查询而不用再编写代码将中间数据注册进Spark-sql后才进行查询。


# QuickStart

下面的QuickStart教程会使用Corgi来对一份CSV格式的数据文件进行查询清洗校验，通过一个完整的ETL清洗教程来向大家展示Corgi的魅力所在。

假设我们有如下CSV数据：

姓名       年龄   性别   职业

咕噜大大 ||  18  || 男 ||  码农  
阿彪     ||  21  || 男 ||  全栈工程师  
阿吉鲁   ||  23  || 男 ||  数据工程师  
阿翔     ||  38  || 男 ||  项目经理  
阿星     ||  40  || 男 ||  前端开发   
阿虎     ||  29  || 男 ||  后端开发  
神乐     ||      || 女 ||  阴阳师  
白藏主   ||      ||    ||  式神  

下载了Corgi项目后，在项目的根目录下执行
```
mvn clean assembly:assembly scala:compile compile package -Dmaven.repo.local=/Users/minghao/.m2/repository-scala-2.1.1/
```
该命令将会在target目录下生成项目的jar包，将jar包放入到libs文件夹中

随后使用Corgi的脚本文件创建工程
$/path/to/corgi/bin/corgi.sh  –p  /path/to/corgi-project

进入到 /path/to/corgi-project/conf/ 目录中编辑corgi-conf.xml文档


corgi-conf.xml
```
<configuration>
  <property>
     <name>spark.sql.warehouse.dir</name>
     <value>/path/to/warehouse </value>
  </property>
  <property>
     <!-- only accept local path -->
     <name>javax.jdo.option.ConnectionURL</name>
     <value>/path/to/metastore_db </value>
  </property>
  <property>
     <name>spark.master</name>
     <value>local </value>
  </property>
  <property>
     <name>spark.submit.deployMode</name>
     <value>client</value>
  </property>
  <property>
     <name>spark.app.name</name>
     <value>corgi-test </value>
  </property>
  <property>
     <name>spark.driver.cores</name>
     <value>2 </value>
  </property>
  <property>
     <name>spark.executor.memory</name>
     <value>2g </value>
  </property>
  <property>
     <name>spark.executor.cores</name>
     <value>3 </value>
  </property>
   <property>
     <name>spark.executor.instances</name>
     <value>2 </value>
  </property>
   <property>
     <name>spark.yarn.queue</name>
     <value>defualt </value>
  </property>
</configuration>
```

接下来进入到/path/to/corgi-project/channel/ 目录中编辑main.cha.xml 和person.cha.xml文件，如果数据源要处理的是CSV格式的数据，还需要指定和编写CSV数据所对应的元数据配置文件。

person.cha.xml 
```
<channel>

<source name="People" path="/path/to/data.csv" type="csv" 
meta_data="/path/to/ people-metadata.xml" />
 
  	<transactions>
  		<transaction type="sql" name="NormalPeople" >
  			select * from people where occupation!=’阴阳师’
  		</transaction>
  	    <transaction type="sql" name="SeniorPeople" >
  		    select * from NormalPeople where age>30
  	    </transaction>
</transactions>

<sink name=" Result " transaction_name=" SeniorPeople " path="/path/to/sink/data" 
type="parquet" external_table="true" /> 
 
</channel>
```
编辑data.csv 数据文件对应的元数据xml文件people-metadata.xml

people-metadata.xml
```
<metadata>
  <configuration delimiter="||" />
  <columns>
    <column type="String" nullable="true" >name</column>
    <column type="String" nullable="true" >age</column>
    <column type="String" nullable="true" >gender</column>
    <column type="String" nullable="true" >occupation</column>
  </columns>
</metadata>
```

最后就是编辑入口文件main.cha.xml

```
<channel-chain>
  <channels>
    <channel>
      person.cha.xml 
    </channel>  
  </channels>
</channel-chain>
```
当所有配置文件都准备好后，只要执行下面的命令就可以启动ETL任务

$/path/to/corgi/bin/corgi.sh  –r  /path/to/corgi-project


当整个ETL流程执行完毕后，用户还可以编写数据校验文件来对ETL过程中产生的中间过程数据或者结果数据进行校验工作。
如果要执行校验任务，用户需要进入到/path/to/corgi-project/validation/ 目录下中编辑main.valid.xml 和person.valid.xml文件。目前Corgi提供的校验功能只有简单的输入输出校验平衡功能，后续功能会持续开发中。

person.valid.xml
```
<validation>
	<transactions>
		<transaction  type="validation" name="verify-normal-people" 
          function="input_output" source="People" target="NormalPeople ">
       </transaction>
	</transactions>
</validation>
```
main.valid.xml
```
<validation-chain>
	<validations>
		<validation>
			person.valid.xml
		</validation>
	</validations>
</validation-chain>
```
都配置文件都准备完毕后，就可以输入下面的命令执行校验操作

$/path/to/corgi/bin/corgi.sh  –v  /path/to/corgi-project


除了通过validation模块对数据进行检查外，Corgi还为用户提供了更加快捷的方式去对Channel的中间数据进行查询，那就是Query模块的主要功能。通过以下命令和SQL语句的配合，一样可以快速查询到中间过程或者结果数据

$/path/to/corgi/bin/corgi.sh  –q  /path/to/corgi-project  “select * from SeniorPeople”


Corgi在后续版本中会继续完善各个模块的功能，并加入增量数据处理功能和定时调度功能，请大家敬请期待。
