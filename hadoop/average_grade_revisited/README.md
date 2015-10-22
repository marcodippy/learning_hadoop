# Exercise #

Some improvements on the [previous project](../average_grade)

Given a semicolon delimited file containing a list of grades of students in several courses (not ordered), calculate the average grade of each student for each course (each student has three grades per course). Output file must be comma separated and its record must be ordered per course (ascending order) and student (descending order).  
  
New requirements:
* Improve job performance
* Handle eventual wrong input records (missing fields or invalid data: *grade* must be comprised between 60 and 100).
* Count also the number of:
 * distinct courses appearing in the file
 * distinct students 
 * students for each course
 * courses for each student
 
   
- - - - 
- - - - 

## Notes: ##
### Improving performance ###
#### In-memory combiner ####
This pattern consists in combining results within the Mapper, before the keys are actually emitted.  
The benefits are:
* *Combining phase* is guaranteed to happen
* reduced number of emitted keys to shuffle, sort and reduce (this means less overhead in disk/network I/O and in object creation/destruction/serialization/deserialization)

The drawbacks is that the memory of the Mapper is a limit: if your intermediate data structure is too big you should implement some flushing mechanism.

#### Raw Comparator ####
There is an possible optimization for the [Comparator](../average_grade/src/main/java/org/mdp/learn/hadoop/average_grade/CourseAndStudentKeyComparator.java) implemented in the previous project: in this case, objects need to be deserialized to be compared, while is it possible performing this comparison looking only at their serialized representation.  
Let's look at the implementation:
```java 
public int compare(byte[] bytes_1, int start_1, int length_1, byte[] bytes_2, int start_2, int length_2)
```
- - - - 

### Bad input record handling ###

- - - -

### Counters ###

- - - -