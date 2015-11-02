# Exercise #
Implement a basic inverted index.

- - - - 
- - - - 

## Notes: ##
The structure of a simple inverted index is:

	t1 --- (d1, p) -> (d3, p) -> (d12, p) ...
	t2 --- (d1, p) -> (d7, p) -> (d9, p)  ...
	t3 --- (d3, p) -> (d5, p) -> (d13, p) ...
	...

where *t* are terms, *d* are the IDs of the documents in which the term appears and *p* are the payloads (a payload is an information about occurrences of the term in the document, can be a boolean, a frequency, etc). The couple (d,p) is a *posting*.  
Generally, postings are sorted by document id, and document ids are *not arbitrary* numeric values.  

Mappers emit postings keyed by (term, docId), the execution framework groups postings by term and sort by docId, and the reducers aggregate and write postings lists to disk.  

A fundamental part of building inverted indexes is compressing the output; here only a small improvement has been implemented: instead of storing each document id in its original form, we can express it as the difference between the current docId and the previous one.