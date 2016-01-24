package wip.wikidata_neo4j_importer;

public class Config {
    public static int restartNodeNum = 2000000; // after "restartNodeNum" items, we restart neo4j (when importing nodes)
    public static int printNodeNum = 5000;      // after "printNodeNum" items, we print progress (when importing nodes)

    public static int restartEdgeNum = 2000000; // after "restartEdgeNum" items, we restart neo4j (when importing edges)
    public static int printEdgeNum = 5000;      // after "printEdgeNum" items, we print progress (when importing edges)

    /*
    Every node is either an item or property in wikidata, and each of them has 2 ids:
        1. id for neo4j
        2. id for wikidata (their original id in wikidata)
    For items Qxx, its neo4j id is 1xx (e.g. neo4j id for Q1 is 11).
    For property Pxx, its neo4j id is 2xx (e.g. neo4j id for P31 is 231).
    This conversion can facilitate import procedure.
     */
    public static int itemPrefix = 1;
    public static int propPrefix = 2;
}
