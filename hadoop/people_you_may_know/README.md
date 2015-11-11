# Exercise #
Given an input file containing rows composed by
	
	userId - list of friends (comma separated)

give to each user a list of "people he may know".  
	  
- - - -
- - - -

## Notes: ##
The basic idea is: if two "not friend" people have a set of mutual friends the MapReduce solution should recommend that they may be connected to each other. 

### Mapper ###
Supposing that the mapper receives the row `A -> B,C,D` the first thing to do is emitting all the k-v pairs to identify people who are already connected:

	(A, (B, "FRIEND"))
	(A, (C, "FRIEND"))
	(A, (C, "FRIEND"))
	
(I used the string "FRIEND" as a "special" value to identify the existing relationship)  
Then, it emits the "potential" friends:

	(C, (B, A))		- B is a potential friend of C (because A is the mutual friend)
	(B, (C, A))		- C is a potential friend of B (because A is the mutual friend)
	
	(D, (B, A))		- B is a potential friend of D
	(B, (D, A))		- D is a potential friend of B
	
	(D, (C, A))		- C is a potential friend of D
	(C, (D, A))		- D is a potential friend of C
	
- - - -

### Reducer ###
The reducer receives, for each user, the list of its actual friends ((A,"FRIENDS"), (x,"FRIENDS"), ...) and the list of its potential friends ((B, (D, A)), (B, (C,A)), ...).  
Supposing that the reduce receives `B -> [(A,"FRIENDS"), (C,"FRIENDS"), (D,A), (E,A), (E,C), (C,A)]` the results will be:

	B -> D(1:[A]), E(2:[A,C])	- D and E are potential friends; 2:[A,C] are the number of mutual friends and the mutual friends
	
This can be done by using an associative array, containing the potential friend as the key, and the list of mutual friends as the value; while cycling over the reduce values, when an existing friendship is encountered you put *null* as the value in the associative array (and ensure that *null* will never be overridden).	
- - - -


