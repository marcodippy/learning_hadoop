# Exercise #
Given a list of transactions with the associated products, find, for each product, the products that are frequently bought with it (in the same transaction)
- - - -
- - - -

## Notes: ##

### Mapper ###
Mapper emits, for each pair of product within a transaction, a k-v pair composed by:
* (p1, p2) as the key; products in the composed key are sorted because the relationship is bidirectional (easily achievable by sorting the list of products of the transaction)
* 1 as the value 
- - - -

### Reducer ###
The reducer only sums up the values for each pair of product, giving the number of times that two items are bought together.
- - - -


