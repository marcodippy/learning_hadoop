# Exercise #
###Customers Who Bought This Item Also Bought###
The input files is composed by a list of (userId;productId).  
For each product we want to find the N items most commonly purchased by customers who also bough the original product.
- - - -
- - - -

## Notes: ##
The solution is composed by two iterations of MapReduce

### Phase 1 ###
The output of this first phase are lists of all products by the same user. This is easily achievable by emitting (userId,productId) k-v pairs from mappers and grouping the productIds by user in the reducers.
- - - -

### Phase 2 ###
This phase consists in solving the co-occurrences problem on list items.  
In particular I used the *Stripes* design pattern. The mapper takes a list of items bought by the same user and cycles over these items; for each item it emits a k-v pair where the key is the productId and the value is an associative array containing the count of each "co-occurring" product. The reduce performs then the aggregation on these maps and emits the N most sold co-occurring products.
- - - -


