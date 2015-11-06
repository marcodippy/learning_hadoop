# Exercise #
Implement a basic version of the Page Rank algorithm
- - - - 
- - - - 

## Notes: ##
The PageRank of a page *n* is 

	P(n) = JUMPING_FACTOR * ( 1 / totNumberOfPages) + DAMPING_FACTOR * Σ ( P(m) / C(m) )
	
where:
* **JUMPING_FACTOR** is the probability that a random surfer executes a random jump to the page
* **DAMPING_FACTOR** is the probability that a random surfer lands at the page by following a link (it's equal to 1-JUMPING_FACTOR)
* **P(m)/C(m) ** is the probability that a random surfer will arrive at the page *n* from *m* 
* **Σ (P(m)/C(m)) ** is the summation of all the contributions from all the pages that link to *n*
