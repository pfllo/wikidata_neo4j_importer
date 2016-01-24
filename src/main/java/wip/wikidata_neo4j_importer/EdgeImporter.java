package wip.wikidata_neo4j_importer;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class EdgeImporter {

    Map<String, String> propDic = new HashMap<String, String>();
    PrintWriter logWriter;  // log all created new nodes when importing edges
    BatchInserter inserter;

    Label labelItem; // group nodes of items
    Label labelProp; // group nodes of properties

    public int nodeCreatedCnt = 0;  // number of nodes created when importing edges

    public EdgeImporter(String pathNeo4jDatabase, String propDumpPath, String logPath) throws IOException {
        labelItem = DynamicLabel.label("Item");
        labelProp = DynamicLabel.label("Property");
        logWriter = new PrintWriter(logPath);

        // readPropDic(propDumpPath);   // you can uncomment this line if you want to use the property dump.

        initializeInserter(pathNeo4jDatabase);
    }

    public void initializeInserter(String pathNeo4jDatabase) throws IOException {
        inserter = BatchInserters.inserter(new File(pathNeo4jDatabase));
    }

    public void importEdge(String itemDocStr) {
        JSONObject obj = new JSONObject(itemDocStr);
        String subjWikidataId = obj.getString("id");
        long subjNodeId = Util.addPrefixToLong(Long.parseLong(subjWikidataId.substring(1)), Config.itemPrefix, 10);

        if (!obj.has("claims")) return;
        JSONObject claimObj = obj.getJSONObject("claims");
        for (String propId : claimObj.keySet()) {
            JSONArray valueArray = claimObj.getJSONArray(propId);
            String snakType = valueArray.getJSONObject(0).getJSONObject("mainsnak").getString("snaktype");
            if (!snakType.equals("value")) continue;    // ignore some value and no value
            String valueType = valueArray.getJSONObject(0).getJSONObject("mainsnak")
                    .getJSONObject("datavalue").getString("type");

            // Only import edges between entities
            // Ignore other property values
            if (valueType.equals("wikibase-entityid")) {
                RelationshipType tempPropType = DynamicRelationshipType.withName(propId);

                for (int i = 0; i < valueArray.length(); i++) {
                    JSONObject mainSnakObj = valueArray.getJSONObject(i).getJSONObject("mainsnak");
                    if (!mainSnakObj.getString("snaktype").equals("value")) continue; // ignore novalue and somevalue
                    if (!mainSnakObj.getJSONObject("datavalue").getString("type").equals("wikibase-entityid"))
                        continue;

                    String nodeType = mainSnakObj.getJSONObject("datavalue").
                            getJSONObject("value").getString("entity-type");
                    long objNodeId = mainSnakObj.getJSONObject("datavalue")
                            .getJSONObject("value").getLong("numeric-id");
                    String objWikidataId = objNodeId + "";
                    if (nodeType.equals("item")){
                        objNodeId = Util.addPrefixToLong(objNodeId, Config.itemPrefix, 10);
                        objWikidataId = "Q" + objWikidataId;
                    } else {
                        objNodeId = Util.addPrefixToLong(objNodeId, Config.propPrefix, 10);
                        objWikidataId = "P" + objWikidataId;
                    }


                    // Create subject node if not exist (normally, this shouldn't be triggered)
                    if (!inserter.nodeExists(subjNodeId)) {
                        Map<String, Object> nodeProperties = new HashMap<String, Object>();
                        nodeProperties.put("wikidataId", subjWikidataId);
                        inserter.createNode(subjNodeId, nodeProperties, labelItem);

                        nodeCreatedCnt += 1;
                        logWriter.write("Inserted: " + subjWikidataId+ ", from " + subjWikidataId + "\n");
                    }
                    // Create object node if not exist (normally, this shouldn't be triggered)
                    if (!inserter.nodeExists(objNodeId)) {
                        Map<String, Object> nodeProperties = new HashMap<String, Object>();
                        nodeProperties.put("wikidataId", objWikidataId);
                        inserter.createNode(objNodeId, nodeProperties, labelItem);

                        nodeCreatedCnt += 1;
                        logWriter.write("Inserted: " + objWikidataId + ", from " + subjWikidataId + "\n");
                    }

                    inserter.createRelationship(subjNodeId, objNodeId, tempPropType, null);
                }
            }
        }
    }

    public void shutDownNeo4j(){
        inserter.shutdown();
    }

    public void close() {
        inserter.shutdown();
        logWriter.close();
    }

    /**
     * Read the property dump produced when importing nodes.
     * @param propDumpPath  dump file path
     * @throws IOException
     */
    private void readPropDic(String propDumpPath) throws IOException {
        Scanner scanner = new Scanner(Paths.get(propDumpPath));
        while (scanner.hasNext()) {
            String propDocStr = scanner.nextLine();
            if (propDocStr.equals("")) break;

            JSONObject obj = new JSONObject(propDocStr);
            String wikidataId = obj.getString("wikidataId");
            String label = obj.getString("label");
            propDic.put(wikidataId, label);
        }
    }
}
