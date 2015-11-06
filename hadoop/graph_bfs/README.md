# Exercise #
Find the shortest path from a source node to all other nodes in an unweighted graph.
- - - - 
- - - - 

## Notes: ##
Graph algorithms in MapReduce are generally iterative.  
Like in the Dijkstra shortest path algorithm, everything starts with setting to *infinite* the distance to each node except the source node (which is set to 0). At the first iteration, mappers "discover" all the nodes connected to the source: they emit key-value pairs composed by (nodeId, currentDistance + 1) for each "neighbor" node. Reducers receive and group the values for each node, select the minimum and write out the node with its new distance value (for that reason we need to pass the node structure "as is" from the mapper).  
At the second iteration, the "neighbors" of the nodes just discovered are taken in consideration and the process is repeated; each iteration expands the "search frontier" by one hop.  
The algorithm is terminated when all the nodes in the graph have been discovered (there are no distances set to *infinite*); this is done leveraging the hadoop counters: the reducers keep the count of the times that an *infinite* value is changed, whereas the driver orchestrates the process.  
  
  
At this point we've found the shortest distances, not the actual shortest paths. Go to the [next exercise](../dijkstra) for the solution!