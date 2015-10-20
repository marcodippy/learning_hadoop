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
My first attempt to run the job was  
`hadoop jar word_count-0.0.1-SNAPSHOT.jar org.mdp.learn.hadoop.word_count.WordCountDriver input_word_count output_word_count`  
and it didn't work (of course). Since I created a different maven module to hold some common classes I had to specify the jar dependencies for the job with `-libjars`  
`hadoop jar word_count-0.0.1-SNAPSHOT.jar org.mdp.learn.hadoop.word_count.WordCountDriver -libjars ../../commons/target/commons-0.0.1-SNAPSHOT.jar input_word_count output_word_count`  
but this wasn't enough: `-libjars` makes your third-party JARs available to the remote map and reduce task JVM's but not to the client JVM. For this reason you have to set the `HADOOP_CLASSPATH` before executing your job: `export HADOOP_CLASSPATH=/path/jar1:/path/jar2`
- - - - 

### Combiner ###





