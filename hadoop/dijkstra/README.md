# Exercise # 
(Continues from the [previous exercise](../graph_bfs))  

You've given a file with airports and flights: 

	MPX-[(JFK; 190),(HAV; 80),(FCO; 40)]
	JFK-[(MIA; 60)]
	HAV-[(MIA; 120),(LHR; 160),(JFK; 140)]
	MIA-[(LHR; 135),(MPX; 110)]
	LHR-[(DCA; 165),(MPX; 50)]
	DCA-[(FCO; 124),(CDG; 140)]
	FCO-[(LHR; 85),(HAV; 115),(CDG; 90)]
	CDG-[(DCA; 170),(LHR; 35)]
where:
* "-" is the field separator
* the first field is the departure airport
* the second field is a list of reachable airport with the relative cost of the flight

Find the cheapest way to flight from Milan (MPX) to New York (JFK). As we are young and strong, we don't care about stopovers :) 
	
- - - - 
- - - - 

## Notes: ##
This time the graph is weighted!  
There are only some small changes we have to apply to the [previous exercise](../graph_bfs):
* instead of emitting (nodeId, currentDistance + 1) for each neighbor node, mapper emits (nodeId, currentDistance + edgeWeight)
* along with the distance value, mappers emit the path to the neighbor node.
* the stopping criteria for the iterations changes! The node is not "completely" discovered at the first time it is encountered, so we have to check, at each iteration, how many times a distance changes: the process terminates when there are no more values changing in the graph.
