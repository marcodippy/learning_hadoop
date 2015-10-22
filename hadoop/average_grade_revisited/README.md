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
Each object to be compared is identified with:
* `byte[] bytes` - You can easily figure out what is it.
* `int start` - The object under comparison's starting index.
* `int length` - The length of the object in the byte array.
Implementing a RawComparator is a little bit tricky and requires a bit of attention: the Text object is serialized prepending to the actual content a VIntWritable containing the size of the content; this VIntWritable has a (serialized) size that can vary from 1 to 5 bytes, and the actual size is stored in the leading byte of the array.
So, considering that our custom type contains two Text object, a possible byte representation can be:  

VInt length (1st obj) | VInt (1st obj) | text (1st obj) | VInt length (2nd obj) | VInt (2nd obj) | text (2nd obj) 
------------- | ------------- |------------- | ------------- |------------- | ------------- |------------- | -------------  
XXX  | YYY  | ZZZ  | ...  | ...  | ...  
   
Where
* `XXX` is the total length of the VInt object representation (hence it defines the starting point for the actual text)
* `YYY` is the total length of the text (then XXX + YYY gives the total length of the first Text object in our custom type)
* `ZZZ` is the text content

The rest is pretty obvious,[see the code](./src/main/java/org/mdp/learn/hadoop/average_grade_revisited/CourseAndStudentKeyComparator.java)!

- - - - 

### Bad input record handling ###

- - - -

### Counters ###

- - - -