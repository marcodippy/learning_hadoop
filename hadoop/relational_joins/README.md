# Exercise #
Join two "tables" with hadoop

- - - - 
- - - - 

## Notes: ##
### Reduce-side join ###
Quite simple, mappers emit rows from both datasets using the join key as the intermediate key, whereas reducers will perform the actual join. This means that all the data in the datasets need to be shuffled to the reducers across the network.

#### one to one ####
Mapper emits a key composed by the actual join key and a value corresponding to the origin table of the row, and the row itself as the value.
Leveraging the secondary sort mechanism, we can make the rows arriving to the reducer sorted by the *origin table* and grouped by the *join key*; the reducer will receive data like:  
	
	k1 -> [(x, L), (y, R)]  
	k1 -> [(z, L)]  

where *L* and *R* are the values associated to the origin table and *x*,*y* and *z* are the rows.  
Reducer can now join the rows by easily concatenating the two element in the array of values.
 
#### one to many / many to many ####
The implementation for 1toN and NtoM is not so different from the previous approach. We drop the custom grouping comparator in order to present data to the reducer in the format

	(k1, L) -> [x, y, z]
	(k2, R) -> [w, j]
	
Now we can keep in memory the rows of the first table and join them with the corresponding rows of the second one. Of course, we are assuming that the data fits into the memory.  

- - - -

### Map-side join aka *Merge join* ###
This approach works when the datasets to be joined are already sorted by the same (join) key and have the same number of partitions.  
It consists in scanning both datasets simultaneously (piece by piece) and merge each "partition" of the first table with the corresponding partition of the second one. No reducer is required (no data to be shuffled over the network), so this approach is far more efficient.  

All the magic here is done by hadoop *CompositeInputFormat* class specified in the driver.

- - - - 

### Memory backed join ###
We can use this approach when one of the datasets is small enough to fit completely into memory on each node: every mapper will keep it into memory in an associative array (*hash join*) while processing the data of the other datasets. Again, there's no need for reducers.  
- - - -  
