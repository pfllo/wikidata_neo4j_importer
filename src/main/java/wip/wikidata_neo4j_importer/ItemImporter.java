package wip.wikidata_neo4j_importer;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ItemImporter {

    Label labelItem; // group nodes of items
    Label labelProp; // group nodes of properties

    // Write all properties to a file.
    // We don't use them during importing actually.
    // But it can be convenient and efficient to use this dump,
    // rather than original json file if you want to add additional information to edges.
    PrintWriter propertyWriter;

    BatchInserter inserter;

    public ItemImporter(String pathNeo4jDatabase, String propDumpPath) throws IOException {
        labelItem = DynamicLabel.label("Item");
        labelProp = DynamicLabel.label("Property");
        propertyWriter = new PrintWriter(propDumpPath);

        initializeInserter(pathNeo4jDatabase);
    }

    public void initializeInserter(String pathNeo4jDatabase) throws IOException {
        inserter = BatchInserters.inserter(new File(pathNeo4jDatabase));
        // inserter.createDeferredSchemaIndex(labelItem).on("wikidataId").create();
    }

    public void importItem(String itemDocStr, Boolean isItem) {
        // Extract key information from json string
        JSONObject obj = new JSONObject(itemDocStr);
        String wikidataId = obj.getString("id");
        String datatype = getDatatype(obj);    // only exists in property
        String label = getEnLabel(obj);
        String description = getEnDescription(obj);
        String aliasStr = getEnAliases(obj);

        // Construct property map of current node
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("wikidataId", wikidataId);
        if (!datatype.equals("")) properties.put("datatype", datatype);
        if (!label.equals("")) properties.put("label", label);
        if (!description.equals("")) properties.put("description", description);
        if (!aliasStr.equals("")) properties.put("aliases", aliasStr);

        // Generate id of current node and insert it
        long nodeId = Long.parseLong(wikidataId.substring(1));
        if (isItem) {
            nodeId = Util.addPrefixToLong(nodeId, Config.itemPrefix, 10);
            if (!inserter.nodeExists(nodeId))
                inserter.createNode(nodeId, properties, labelItem);
        } else {
            nodeId = Util.addPrefixToLong(nodeId, Config.propPrefix, 10);
            if (!inserter.nodeExists(nodeId))
                inserter.createNode(nodeId, properties, labelProp);
        }

        // If current document is a property, dump it
        if (!isItem) {
            JSONObject resObj = new JSONObject();
            resObj.put("wikidataId", wikidataId);
            resObj.put("label", label);
            resObj.put("datatype", datatype);
            resObj.put("description", description);
            resObj.put("aliases", aliasStr);

            propertyWriter.write(resObj.toString() + "\n");
        }
    }

    public void shutDownNeo4j(){
        inserter.shutdown();
    }

    public void close() {
        inserter.shutdown();
        propertyWriter.close();
    }

    private String getDatatype(JSONObject obj) {
        if (!obj.has("datatype")) return "";
        return obj.getString("datatype");
    }

    private String getEnLabel(JSONObject obj) {
        if (!obj.has("labels")) return "";
        if (!obj.getJSONObject("labels").has("en")) return "";
        return obj.getJSONObject("labels").getJSONObject("en").getString("value");
    }

    private String getEnDescription(JSONObject obj) {
        if (!obj.has("descriptions")) return "";
        if (!obj.getJSONObject("descriptions").has("en")) return "";
        return obj.getJSONObject("descriptions").getJSONObject("en").getString("value");
    }

    private String getEnAliases(JSONObject obj) {
        if (!obj.has("aliases")) return "";
        if (!obj.getJSONObject("aliases").has("en")) return "";

        JSONArray aliases = obj.getJSONObject("aliases").getJSONArray("en");
        String aliasStr = "";
        for (Object aliasObj : aliases) {
            String tempAlias = ((JSONObject) aliasObj).getString("value");
            aliasStr += tempAlias + "\n";
        }
        return aliasStr.substring(0, aliases.length()-1);
    }

}
