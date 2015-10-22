# Exercise #

Given a semicolon delimited file containing a list of grades of students in several courses (not ordered), calculate the average grade of each student for each course (each student has three grades per course). Output file must be comma separated and its record must be ordered per course (ascending order) and student (descending order). 

## Example: ##
### Input ###
  Database; Marco; 100  
  Algorithms; Fabio; 84  
  Database; Marco; 90  
  Algorithms; Marco; 88  
  Algorithms; Marco; 84  
  Database; Marco; 95  
  Algorithms; Fabio; 80  
  Database; Fabio; 82  
  Database; Fabio; 84  
  Algorithms; Marco; 86  
  Database; Fabio; 80  
  Algorithms; Fabio; 82  
  
### Output ###
  Algorithms, Marco, 86  
  Algorithms, Fabio, 82  
  Database, Marco, 95  
  Database, Fabio, 84
  
- - - - 
- - - - 

## Notes: ##
### Map ###
Mapper emits a key-value pair for each row in the file in this form ({course, student}, grade).
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
We want all the records related to a specific course appearing in the same output file; to achieve that, a custom partitioner that calculates the hash only on the course has been created: in this way all the records for a course will end up in the same partition.
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




