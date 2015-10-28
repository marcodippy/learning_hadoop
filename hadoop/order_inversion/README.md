# Exercise #
Calculating the co occurrence matrix by counting the occurrences of each pair of words (as shown in the (Previous exercise)[../co_occurrence_matrix]) has a drawback: this approach doesn't take into account the fact that some words appear more frequently than others (Word W1 may co-occur frequently with W2 simply because one of the words is very common).  
We want then to convert absolute counts into relative frequencies:
  
	f(w1|w2) = numOccurrences(w1,w2) / numOccurrences(w1,*) 
	
	* numOccurrences(w1,w2) -> number of times the pair (w1,w2) is observed
	* numOccurrences(w1,*) -> known as Marginal, number of times that the word w1 co-occurs with anything else
	
- - - -
- - - -

## Notes: ##
We can apply again the patterns seen in the previous exercise (Pairs and Stripes).

### Stripes ###
Solving this problem with Stripes is straightforward, we can reuse the code of previous exercise and modify only the reducer: the marginal can be easily found by summing all the counts in the associative array for the word, then you only need to divide all the single joint count by the marginal and the word is done.  
    
- - - -

### Pairs ###
Pairs approach is not a cakewalk as the Stripes one: here the reducer gets the (w1,w2) pair as the key and the count as the value, hence it's not possible to calculate the marginal until all the counts for (w1, \*) has arrived. We could keep the results in an in-memory buffer, but there is a better solution: in addition to the ((w1, wX), 1) key-value pairs, the mapper emits a ((w1,\*), X) pair representing the total number of times that w1 has been observed. With a custom partitioner that takes into account only the term w1, we can assure that all the data associated to w1 will end up in the same reducer, whereas with a custom comparator we can make the ((w1,\*), X) pairs arrive to the reducer before any other key-value pair, so that we can compute the marginal before the joint count (this is the *core* of the **Order inversion** pattern).  
 
- - - -





