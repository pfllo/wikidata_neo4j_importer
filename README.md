#Introduction
This project import wikidata json.bz2 dump into neo4j.

Note that, this project only import English data and also ignores references, relations between properties and properties whose range is note wikidata item. For example, we ignore properties like "image(P18)", because its range is string.

However, it is convenient to extend this project and add these information if you want.

#Implementation Overview
commons-compress (from org.apache.commons) to read bz2 file

BatchInserter (form java neo4j interface) to import data

json (from json) to process json strings

Our implementation is a two-step procedure:

1. Import all nodes (items and properties)

2. Import all edges (properties) between them

#Statistics
The 20160118 dump (wikidata-20160118-all.json.bz2), which is around 4G, takes 4.3G after imported to neo4j. (Note: not all data are imported, see Introduction part)

It processes around 2500 items per second, and 3 hours to import the whole dump (5113 seconds to import nodes, 6018 seconds to import edges).

It seems that parsing json strings is very time consuming compared to importing data. So, one possible improvement is probably to create nodes and add edges in one pass.

#Usage:
1. Import this project.

2. Use maven to download dependencies.

3. Run the Runner class, parameters are:

    param 1: path of wikidata dump

    param 2: path of neo4j database directory

    param 3: enter 'node' or 'edge', indicating whether import node or edge


