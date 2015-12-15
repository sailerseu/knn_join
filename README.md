# knn_join
这是knn_join基于Hadoop和Spark实现的java源代码，主要分为三个目录，即centralized，Hadoop和Spark，相应的文件夹下就是其对应的源码部分
centralized目录下面值包含一个文件NavieKnn_Topk，实现了传统的knn算法和改进了的算法；Hadoop部分包含有三个部分的源码，即naiveknn,naiveknn_k和ballknn；Spark包含的源码有naiveknn_k和ballknn
Hadoop的相关源码可以在打包后，通过hadoop jar **** 进行运行
Spark的执行需要借助于yarn-master
spark-submit --class xxx.xxx  --master yarn-cluster ....



This is a repository for knn join, and they could be orgonized into two groups, i.e. hadoop based and Spark based.

