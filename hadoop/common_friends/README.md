# Exercise #
Identify common friends among all pairs of users.
The input file contains rows composed by
	
	userId - list of friends (comma separated)
   
- - - -
- - - -

## Notes: ##
In this algorithm, mappers cycle over the list of friends and emit k-v pairs composed by:
* (userId,friendId) as the key; the values of the pair must be sorted since the relationship is bidirectional
* the complete list of friends as the value
Reducers receives (userId,friendId) as key and the lists of friends of the two users as values, so the common friends can be easily retrieved by comparing the these two lists.
- - - - 
- - - - 
