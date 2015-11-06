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

  
The structure of the algorithm is similar to the [parallel breadth-first search](../graph_bfs): at each iteration each node passes its PageRank contribution to other nodes that it is connected to; the process terminates when PageRank values don't change anymore (convergence within some tolerance), or until an arbitrary configured max number of iterations is reached.  

The first step is to initialize a uniform distribution of PageRank values across nodes (you can assign to each node a PR value of 1/totNumberOfNodes).  
Since we begin with the sum of PageRank values across all nodes equal to one, the sum of all the update PageRank values should remain 1 at the end of each iteration. There could be a problem with dangling nodes (pages without outgoing links): their PageRank values cannot be passed to other nodes and an amount of PR is lost.  
For that reason, a second MapReduce job is responsible to redistribute evenly this "lost" PageRank amount across all nodes: that is accomplished with mappers that update the PR value for each node according to the following formula:

	p' = JUMPING_FACTOR * (1 / totNumberOfPages) + DAMPING_FACTOR* (m/totNumberOfPages + p)
	
where **m** is the missing PageRank mass.  
Mappers are responsible to keep trace of missing PageRank mass for each dangling node (this is done with hadoop counters).  
  
So, each iteration is composed by two MapReduce jobs, one to pass the PageRank along graph edges and a second one to take care of dangling nodes. 