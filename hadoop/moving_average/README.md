# Exercise #
  
You have a file containing the daily prices of flights with this structure:  
`Vendor; departure airport; arrival airport; day (of selling); airline; price`  
Calculate the simple moving average of the prices of a flight (independently from its vendor or airline) over a period of N days.  

- - - - 
- - - - 

## Notes: ##


### Job Chaining ###
Since the input file contains multiple prices for each flight/day (prices are per Vendor and Airline), we need a first job to calculate the average daily price for the route. The output of this job will be processed by a second job that calculate the actual moving average. 
One way to implement a job chain is to use the JobControl class (the workflow "orchestrator") and ControlledJob (a wrapper/monitor for the single job)

  
#### The algorithm ####
The core of the algorithm consists in making all the prices for a flight arriving to the reducer ordered by day and calculating the average of the values appearing in the specified time window.  
  
  
The first part is implemented with the classic Secondary Sort technique:
* [MovingAverageKeyComparator](./src/main/java/org/mdp/learn/hadoop/moving_average/MovingAverageKeyComparator.java) orders the keys by flights and day
* [MovingAverageKeyGroupingComparator](./src/main/java/org/mdp/learn/hadoop/moving_average/MovingAverageKeyGroupingComparator.java) keeps in consideration only the flight for grouping all the prices/day under the same key
* [MovingAverageKeyPartitioner](./src/main/java/org/mdp/learn/hadoop/moving_average/MovingAverageKeyPartitioner.java) partitions the keys by flight (we want be sure that all the data relative to a flight are processed by the same reducer)


The calculation of the moving average is done in the [MovingAverage](./src/main/java/org/mdp/learn/hadoop/moving_average/MovingAverage.java) class; using a FIFO queue to hold the N most recent prices (N is the size of the window), it can easily calculate the average price each time that a new value arrives.  

- - - - 
