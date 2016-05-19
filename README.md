# WordCount-with-MapReduce-model
Simplified map-reduce model


> put the directory (wordCount) in the same directory with "zookeeper-3.4.6", like xxx/zookeeper-3.4.6, xxx/wordCount
> make
> first delete the nodes in zookeeper with:
	java -cp ../zookeeper-3.4.6/lib/*:../zookeeper-3.4.6/zookeeper-3.4.6.jar:. DeleteZK
> run the code with: 
	java -cp ../zookeeper-3.4.6/lib/*:../zookeeper-3.4.6/zookeeper-3.4.6.jar:. WordCount [process id (0 ~ 4)] [input folder]
  for example: java -cp ../zookeeper-3.4.6/lib/*:../zookeeper-3.4.6/zookeeper-3.4.6.jar:. WordCount 0 ../2015-project/demo
 
  The client port is 3922.

