# Exercise #
Given a fine containing some text, build the co-occurrence matrix.

    The co-occurrence matrix of a corpus is a square NxN matrix where N is the number of unique words  
    in the corpus. A cell m(i,j) contains the number of times word w(i) co-occurs with word w(j)  
    within a specific context (a natural unit such as a sentence, paragraph, or a document,  
    or a certain window of K words)

- - - -
- - - -

## Notes: ##
To solve this problem we can use two pattern: **Pairs** and **Stripes**

### Pairs ###
With this approach, a mapper emits key-value pairs with each co-occurring word pair (within the specified window or context) as the key and the integer *one* as the value, while the reducer simply sums up all the values associated with the key.  
For example, for the text:  
`a b c a d c a d`  
with a window size of 2 words, a Mapper will emits:  

	((a, b) , 1)
	((a, c) , 1)
	((b, a) , 1)
	((b, c) , 1)
	((b, a) , 1)
	((c, a) , 1)
	((c, b) , 1)
	((c, a) , 1)
	((c, d) , 1)
	((a, b) , 1)
	((a, c) , 1)
	((a, d) , 1)
	((a, c) , 1)
	((d, c) , 1)
	((d, a) , 1)
	((d, c) , 1)
	((d, a) , 1)
	((c, a) , 1)
	((c, d) , 1)
	((c, a) , 1)
	((c, d) , 1)
	((a, d) , 1)
	((a, c) , 1)
	((a, d) , 1)
	((d, c) , 1)
	((d, a) , 1)

	............
	............

The reducer aggregates the results and produces:

	((a, b) , 2)
	((a, c) , 4)
	((a, d) , 3)
	((b, a) , 2)
	((b, c) , 1)
	((c, a) , 4)
	((c, b) , 1)
	((c, d) , 3)
	((d, a) , 3)
	((d, c) , 3)

Note that each pair corresponds to a **cell** in the word co-occurrence matrix.

- - - -

### Stripes ###
In this approach, instead of emitting a key-value pair with the single co-occurrence information, mapper pre-aggregates this data in an associative array containing, for a specific word, all the neighbors and the relative counters. Only once the "window" (or context) has been completely scanned, mapper emits the key-value pair composed by the word as the key and the associative array as the value. Again, the reduce sums up the intermediate results.  
Using the previous example, mapper will emit:  
`a b c a d c a d`

	(a, {b:1, c:1})
	(b, {a:2, c:1})
	(c, {a:2, b:1, d:1})
	(a, {b:1, c:2, d:1})
	(d, {a:2, c:2})
	(c, {a:2, d:2})
	(a, {c:1, d:2})
	(d, {a:1, c:1})

and the reducer:

	(a, {b:2, c:4, d: 3})
	(b, {a:2, c:1})
	(c, {a:4, b:1, d:3})
	(d, {a:3, c:3})

In contrast to the pairs approach, each final key-value pair represents a **row** in the co-occurrence matrix.

- - - -

Both algorithms can benefit from the use of (in-memory) combiners

