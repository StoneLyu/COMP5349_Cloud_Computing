# COMP5349 assignment 1

## 1. introduction

implement simple data analytic workload using basic feature of MapReduce and Spark framework.  
two workloads you should design and implement against this data set.  
one with MapReduce and the other with Spark.   随意选择一个框架完成期中任意一个。

## 2. input data set description 输入数据类型介绍

数据集包含几个月来，YouTube在以下五个国家的每日最佳趋势的记录。  
每天最多有200条视频被列入趋势列表。  

每个国家的数据被存入一个独立的CSV文件里。  
CSV的每一行代表一条趋势视频的记录。  
如果视频有多日被列入趋势图，那么每个趋势图都会有它自己的记录。  
这个记录包括：video id, title, trending date, publish time, number of views, and so on.  
同样，这个视频记录包括一个```category_id```字段。 在每个国家里，这个```category```有些许不同。  
每个国家还有个json文件，在这个json文件中定义了```categoty ID```和```category name```间的映射。  

## 3. analysis workload description

### 3.1 category and trending correlation(相互关系)

某些视频在多个国家都是趋势视频（很火热），我们有兴趣去知道在分类和重叠的趋势视频之间是否有联系。  
如：UK和CA的用户在音乐分类上有共同的兴趣，但是在体育分类上有不同的兴趣，我们可能会看到UK的热门音乐的3%会出现在CA的热门音乐里。

同时只有0.5%的UK热门视频会出现在CA的列表里。  
在本次工作中，你被要求找出：**在给定的国家对A和B中，对于每一个国家A的分类，有多少视频是热门视频，同时它们中有多大的百分比是国家B中的热门视频。  

>如: Sport; total 152; 17.1% in US

### 3.2 impact of trending on view number 趋势对查看次数的影响

you are asked to find out, for each country, all videos that have greater than or equal to 1000% increase in viewing number between its second and first trending appearance.  
the result should be grouped by country and sorted by discerningly by percent increase.  
the result would look like:  
>DE; V1zTJIfGKaA, 19501.0%  
>...  
>CA; _I_D_8Z4sJE, 8438.1%  

your output may slightly wariate from the above example.  
for instance, you may choose different delimiter character, or you may show fold increase instead of percentage increase.

## 4. coding and execution requirement

你的implement应该利用相应框架提供的特性/功能  
尤其(in particular)，你应并行化大部分操作。  
Hadoop的实现应该运行在伪分布式模式。  
Spark实现应该运行在单机的 standalone cluster(独立集群) or YARN cluster(YARN集群)上  

## 5. compliance statement

### 5.1 评价标准：

- workload 1:

1. 输出: 3.0 pts
产生正确的结果，由正确的格式和正确的顺序
2. 总体设计: 1.0 pts
这指的是job是数量和每个job被设计来做什么。无关数据应该尽早过滤。
3. 细节完成: 3.0 pts
单个job的设计应该是合理的。map, reduce的键值对函数设计应该基于工作负荷。reduce不可以使用不必要的内存数据结构。在map和reduce阶段均允许并行化。shuffle的大小应介于map和reduce之间，且应该合理。
4. 报告: 3.0 pts
总的处理流程必须被清晰阐述(1 point)。输入输出格式必须清晰阐述(1 point)。对于设计的变化必须有合理的解释(0.5 point)。必须指出哪些部分是并行化的(0.5 point)。

- workload 2:

1. 输出: 3.0 pts
产生正确的结果，由正确的格式和正确的顺序
2. 总体设计: 1.0 pts
总体流程必须是合理的。提供总的DAG数据流。无关数据必须尽早过滤。
3. 细节设计: 3.0 pts
必须合理。所使用的RDD的类型的设计必须基于工作载荷。
4. 报告: 3.0 pts
总的处理流程必须被清晰阐述(1 point)。输入输出格式必须清晰阐述(1 point)。对于设计的变化必须有合理的解释(0.5 point)。必须指出哪些部分是并行化的(0.5 point)。