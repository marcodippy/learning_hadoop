# Exercise #

Given a file containing space separated words, count the occurrence of each word.

## Example: ##
### Input ###
to be or not to be  
this is the question 
  
### Output ###
to 2  
be 2  
or 1  
not 1  
this 1  
is 1  
the 1  
question 1  
  
- - - - 
- - - - 

## Notes: ##
### Execute the job ###
hadoop_classpath
hadoop jar average_grade-0.0.1-SNAPSHOT.jar org.mdp.learn.hadoop.average_grade.AverageGradeDriver -libjars ../../commons/target/commons-0.0.1-SNAPSHOT.jar input_average_grade output_average_grade

- - - - 

### CourseAndStudentWritable ###
A custom WritableComparable type has been created to hold the composite key {course, student}.  
Note: if you want to create a custom type that will be used only as a value (and NOT as a key), implementing Writable is enough. 

#### toString() ####
You need to implement the toString() method if you want to use the TextOutputFormat

#### hashCode() ####
hashCode() method is used by the default partitioner (HashPartitioner). You can override this by creating a custom partitioner and configuring the job to use it with the method job.setPartitionerClass()

#### compareTo() ####
compareTo() method is used by the default comparator to define the order of the records depending on the key. You can override this by creating a custom WritableComparator and configuring the job to use it with the method job.setSortComparatorClass()
- - - - 

### CourseAndStudentKeyPartitioner ###
The default partitioner is HashPartitioner, which hashes a record’s key to determine which partition the record belongs in. Each partition is processed by a reduce task. Each reducer create an output file.
We want all the records related to a specific course appearing in the same output file; to achieve that, a custom partitioner that calculates the hash only on the course is created: in this way all the records for a course will end up in the same partition.
Remember that the partition phase happens on the map side. 
- - - -

### CourseAndStudentKeyComparator ###
This custom WritableComparator is used to sort the Map outputs; here we specify that we want the courses sorted in ascending order and the students in descending order (the compareTo() method of CourseAndStudentWritable would have sorted rows by course and student both in ascending order).
This phase happens on the map side.
- - - -

### CourseAndStudentKeyGroupingComparator ###
This happens on the reduce side: records arriving from several mappers must be grouped by the key and this is exactly the meaning of the GroupingComparator. It's configured using job.setGroupingComparatorClass().
Supposing that a mapper emits (K1, V1), (K1, V2), (K2, V3), the reducer should receive (K1,[V1, V2, V3]); this magic is done by the GroupingComparator.
If a custom grouping comparator is not specified for the job, the compareTo() method of the WritableComparable (key) class is invoked. 
Creating a custom GroupingComparator wasn't really needed for this exercise since the default compareTo() method of the CourseAndStudentWritable class already contains the correct grouping logic.   
- - - -

### Reduce ###
Reducer receives data in the form ({course, student}, grades[]) and calculates the average of the grades.  
  
    
- - - -
- - - -

## Flow ##
### Map side ###
Collector --> Partitioner --> Spill --> Comparator --> Local Disk (HDFS) <-- MapOutputServlet
### Reduce side ###
MapOutputServlet --> Copy to Local Disk (HDFS) --> Group --> Reduce




