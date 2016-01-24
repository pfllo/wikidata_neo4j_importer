package wip.wikidata_neo4j_importer;

import java.io.BufferedReader;
import java.io.IOException;


public class Runner {
    public static void main(String[] args) throws IOException {

        if (args.length < 3){
            System.out.println("Usage: java Runner wikidata_dump_path neo4j_db_dir [node/edge]\n" +
                    "param 1: path of wikidata dump\n" +
                    "param 2: path of neo4j database directory\n" +
                    "param 3: enter 'node' or 'edge', indicating whether import node or edge");
            System.exit(1);
        }

        String dataPath = args[0];
        String pathNeo4jDatabase = args[1];
        String mode = args[2];      // whether we are inserting nodes or edges
        String propertyDumPath = "./propertyDump.txt";      // dump all property information in this file
        String edgeLogPath = "./edgeLog.txt";   // log uncreated nodes when adding edges

        int itemCounter = 0;    // number of items processed
        int propCounter = 0;    // number of properties processed
        long startMilli = System.currentTimeMillis();

        // Note: we don't add any index while importing
        // If you need one, you can manually create them with cypher commands like:
        // "create index on :Item(wikidataId)"
        if (mode.equals("node")) {
            ItemImporter itemImporter = new ItemImporter(pathNeo4jDatabase, propertyDumPath);
            try{
                // Reader of compressed wikidata dump
                BufferedReader inputReader = Util.getBufferedReaderForCompressedFile(dataPath);

                while (true) {
                // for (int i = 0; i < 100; i++) {  // this line is for test
                    if (itemCounter!=0 && itemCounter % Config.printNodeNum == 0) {
                        long tempMilli = System.currentTimeMillis();
                        System.out.printf("Processed %d nodes and %d properties. Used %d secondes.\n",
                                itemCounter, propCounter, (tempMilli-startMilli)/1000);
                    }
                    if (itemCounter!=0 && itemCounter % Config.restartNodeNum == 0) {
                        System.out.println("Restarting Neo4j...");

                        itemImporter.shutDownNeo4j();
                        itemImporter.initializeInserter(pathNeo4jDatabase);
                        System.out.println("Neo4j is back!\n");
                    }

                    String tempDocStr = inputReader.readLine();
                    if (tempDocStr==null || tempDocStr.trim().equals("")) break;
                    if (tempDocStr.startsWith("{\"type\":\"property\"")){
                        propCounter += 1;
                        itemImporter.importItem(tempDocStr, false);
                    } else if (tempDocStr.startsWith("{\"type\":\"item\"")){
                        itemCounter += 1;
                        itemImporter.importItem(tempDocStr, true);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                long tempMilli = System.currentTimeMillis();
                System.out.printf("Processed %d nodes and %d properties. Used %d secondes.\n",
                        itemCounter, propCounter, (tempMilli - startMilli) / 1000);
                System.out.println("Shutting Down Neo4j...");

                itemImporter.close();
            }
        } else if (mode.equals("edge")) {
            EdgeImporter edgeImporter = new EdgeImporter(pathNeo4jDatabase, propertyDumPath, edgeLogPath);
            try{
                // Reader of compressed wikidata dump
                BufferedReader inputReader = Util.getBufferedReaderForCompressedFile(dataPath);

                while (true) {
                // for (int i = 0; i < 100; i++) {  // this line is for test
                    if (itemCounter!=0 && itemCounter % Config.printEdgeNum == 0) {
                        long tempMilli = System.currentTimeMillis();
                        System.out.printf("Processed %d items and %d properties. Created %d nodes. Used %d secondes.\n",
                                itemCounter, propCounter, edgeImporter.nodeCreatedCnt, (tempMilli-startMilli)/1000);
                    }
                    if (itemCounter!=0 && itemCounter % Config.restartEdgeNum == 0) {
                        System.out.println("Restarting Neo4j...");

                        edgeImporter.shutDownNeo4j();
                        edgeImporter.initializeInserter(pathNeo4jDatabase);
                        System.out.println("Neo4j is back!\n");
                    }

                    String tempDocStr = inputReader.readLine();
                    if (tempDocStr==null || tempDocStr.trim().equals("")) break;
                    // ignore property when importing edges
                    if (tempDocStr.startsWith("{\"type\":\"property\"")){
                        propCounter += 1;
                    } else if (tempDocStr.startsWith("{\"type\":\"item\"")){
                        itemCounter += 1;
                        edgeImporter.importEdge(tempDocStr);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                long tempMilli = System.currentTimeMillis();
                System.out.printf("Processed %d items and %d properties. Created %d nodes. Used %d secondes.\n",
                        itemCounter, propCounter, edgeImporter.nodeCreatedCnt, (tempMilli-startMilli)/1000);
                System.out.println("Shutting Down Neo4j...");

                edgeImporter.close();
            }
        }

    }
}
