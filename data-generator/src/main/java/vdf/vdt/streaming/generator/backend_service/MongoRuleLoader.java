package vdf.vdt.streaming.generator.backend_service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MongoRuleLoader {

    private static final String CONNECTION_URI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true";
    private static final String DATABASE_NAME = "rule_engine";
    private static final String ACTIVE_COLLECTION = "rules_active";
    private static final String HISTORY_COLLECTION = "rules_history";

    private static final int BATCH_SIZE = 2000;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String jsonFilePath = "data/rules/1000/2026-08-27_14-11-21-301.json";
//        String jsonFilePath = "data/rules/1000000/2026-08-18_17-29-17.json";
        File file = new File(jsonFilePath);

        if (!file.exists()) {
            System.err.printf("[ERROR] Rule file not found: %s\n", file.getAbsolutePath());
            return;
        }

        System.out.printf("[INFO] Starting rule ingestion from: %s\n", file.getAbsolutePath());

        try (MongoClient mongoClient = MongoClients.create(CONNECTION_URI)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> activeCollection = database.getCollection(ACTIVE_COLLECTION);
            MongoCollection<Document> historyCollection = database.getCollection(HISTORY_COLLECTION);

            // Ensure unique ascending indexes on _id
            activeCollection.createIndex(Indexes.ascending("_id"));
            historyCollection.createIndex(Indexes.ascending("_id"));

            long startTime = System.currentTimeMillis();
            System.out.println("[INFO] Starting rule ingestion...");
            processLargeJsonStream(file, mongoClient, activeCollection, historyCollection);
            System.out.printf("[INFO] Completed rule ingestion in %d ms\n", (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            System.err.printf("[FATAL] Rule ingestion failed: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Streams large JSON rule file to avoid JVM Heap exhaustion and processes in batches.
     */
    public static void processLargeJsonStream(
            File jsonFile,
            MongoClient client,
            MongoCollection<Document> activeCollection,
            MongoCollection<Document> historyCollection
    ) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        List<JsonNode> batchNodes = new ArrayList<>(BATCH_SIZE);
        int totalProcessed = 0;
        int batchIndex = 0;

        try (JsonParser parser = jsonFactory.createParser(jsonFile)) {
            JsonToken firstToken = parser.nextToken();

            if (firstToken == JsonToken.START_ARRAY) {
                // Multi-rule JSON array stream
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    JsonNode node = OBJECT_MAPPER.readTree(parser);
                    batchNodes.add(node);

                    if (batchNodes.size() >= BATCH_SIZE) {
                        batchIndex++;
                        processBatch(batchNodes, client, activeCollection, historyCollection, batchIndex);
                        totalProcessed += batchNodes.size();
                        batchNodes.clear();
                    }
                }
            } else if (firstToken == JsonToken.START_OBJECT) {
                // Fallback for single JSON object
                JsonNode singleNode = OBJECT_MAPPER.readTree(parser);
                if (singleNode != null) {
                    batchNodes.add(singleNode);
                }
            }
        }

        // Flush remaining records
        if (!batchNodes.isEmpty()) {
            batchIndex++;
            processBatch(batchNodes, client, activeCollection, historyCollection, batchIndex);
            totalProcessed += batchNodes.size();
        }

        System.out.printf("[SUCCESS] Ingested %d rules into [%s.%s] and [%s.%s]\n",
                totalProcessed, DATABASE_NAME, ACTIVE_COLLECTION, DATABASE_NAME, HISTORY_COLLECTION);
    }

    /**
     * Executes atomic batch upsert to active collection and append to history collection.
     */
    private static void processBatch(List<JsonNode> nodes, MongoClient client,
                                     MongoCollection<Document> activeCollection,
                                     MongoCollection<Document> historyCollection,
                                     int batchIndex) {
        // 1. Extract all rule IDs for batch version lookup
        List<String> ruleIds = new ArrayList<>(nodes.size());
        for (JsonNode node : nodes) {
            String ruleId = node.path("rule_id").asText();
            if (!ruleId.isEmpty()) {
                ruleIds.add(ruleId);
            }
        }

        if (ruleIds.isEmpty()) return;

        try (ClientSession session = client.startSession()) {
            session.startTransaction();

            try {
                // 2. Fetch current versions in a single projection query (1 network round-trip)
                Map<String, Integer> currentVersions = new HashMap<>();
                activeCollection.find(session, Filters.in("_id", ruleIds))
                        .projection(Projections.include("_id", "rule_version"))
                        .forEach(doc -> {
                            String id = doc.getString("_id");
                            Integer version = doc.getInteger("rule_version", 0);
                            currentVersions.put(id, version != null ? version : 0);
                        });

                List<WriteModel<Document>> activeOps = new ArrayList<>(nodes.size());
                List<WriteModel<Document>> historyOps = new ArrayList<>(nodes.size());
                Date now = new Date();
                ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);

                // 3. Build documents with auto-incremented version
                for (JsonNode node : nodes) {
                    String ruleId = node.path("rule_id").asText();
                    if (ruleId.isEmpty()) continue;

                    Document baseDoc = Document.parse(node.toString());
                    baseDoc.remove("rule_version");
                    baseDoc.remove("version");

                    int currentVer = currentVersions.getOrDefault(ruleId, 0);
                    int nextVer = currentVer + 1;

                    // Prepare active document snapshot
                    Document activeDoc = new Document(baseDoc);
                    activeDoc.put("_id", ruleId);
                    activeDoc.put("rule_id", ruleId);
                    activeDoc.put("rule_version", nextVer);
                    activeDoc.put("is_active", true);
                    activeDoc.put("updated_at", now);
                    activeOps.add(new ReplaceOneModel<>(Filters.eq("_id", ruleId), activeDoc, replaceOptions));

                    // Prepare immutable history snapshot
                    Document historyDoc = new Document(baseDoc);
                    historyDoc.put("_id", ruleId + "_v" + nextVer);
                    historyDoc.put("rule_id", ruleId);
                    historyDoc.put("rule_version", nextVer);
                    historyDoc.put("is_active", true);
                    historyDoc.put("created_at", now);
                    historyOps.add(new InsertOneModel<>(historyDoc));
                }

                // 4. Parallel bulk execution across collections
                if (!activeOps.isEmpty()) {
                    activeCollection.bulkWrite(session, activeOps, new BulkWriteOptions().ordered(false));
                    historyCollection.bulkWrite(session, historyOps, new BulkWriteOptions().ordered(false));
                }

                session.commitTransaction();
                System.out.printf("[INFO] Batch #%d committed successfully (%d records)\n", batchIndex, activeOps.size());
            } catch (Exception e) {
                session.abortTransaction();
                System.err.printf("[ERROR] Batch #%d failed and rolled back: %s\n", batchIndex, e.getMessage());
                throw new RuntimeException("Transaction aborted for batch #" + batchIndex, e);
            }
        }
    }
}