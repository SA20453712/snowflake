package com.apple.snowflakemigration.service;

import com.apple.snowflakemigration.model.SnowflakeProperties;
import com.apple.snowflakemigration.util.JSONUtils;
import com.apple.snowflakemigration.util.S3Utils;
import com.apple.snowflakemigration.util.SnowflakeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

@Service
public class SnowflakeService {

    private static Logger log = LoggerFactory.getLogger(SnowflakeService.class);

    @Autowired
    private SnowflakeUtils snowflakeUtils;

    @Autowired
    private JSONUtils jsonUtils;

    @Autowired
    private S3Utils s3Utils;

    private final LinkedBlockingQueue<Runnable> vertexTableCreationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Runnable> edgeTableCreationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Runnable> jsonObjectsQueue = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor vertexTableCreationExecutor;

    private ThreadPoolExecutor edgeTableCreationExecutor;
    private ThreadPoolExecutor jsonObjectsExecutor;

    private Map<String, List<String>> tableNameVertexIdsMap;

    private static final int batchSize = 100;


    public void exportS3ObjectsToSnowflake(SnowflakeProperties snowflakeProperties) {
        s3Utils.getS3Client();
        // List files from vertex and edges folders
        List<S3Object> vertexFiles = s3Utils.listFiles("nodes");
        log.info("S3ToSnowflake.exportS3ObjectsToMySQL(): vertex files size ={}", vertexFiles.size());
        List<S3Object> edgeFiles = s3Utils.listFiles("edges");
        log.info("S3ToSnowflake.exportS3ObjectsToMySQL(): edges files size={}", edgeFiles.size());
        Connection vertexConn = snowflakeUtils.getConnection(snowflakeProperties, snowflakeProperties.getVertexSchema());
        Connection edgeConn = snowflakeUtils.getConnection(snowflakeProperties, snowflakeProperties.getEdgeSchema());
        try {
            tableNameVertexIdsMap = new HashMap<>();
            createVertexTables(vertexFiles, snowflakeProperties, vertexConn);
            tableNameVertexIdsMap.forEach((k, v) -> log.info("S3ToSnowflake.exportS3ObjectsToMySQL(): tableName = {}, vertex ids size = {}", k, v.size()));
            createEdgeTables(edgeFiles, snowflakeProperties, edgeConn);
        } catch (Exception e) {
            log.error("S3ToSnowflake.exportS3ObjectsToMySQL(): Error while exporting S3 objects to MySQL", e);
        } finally {
            try {
                log.info("Closing vertex connection");
                vertexConn.close();
                log.info("Closing Edge connection");
                edgeConn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        // Create tables and insert data for vertex

    }

    public boolean isTableExists(SnowflakeProperties snowflakeProperties, String tableName) {
        Connection vertexConn = snowflakeUtils.getConnection(snowflakeProperties, snowflakeProperties.getVertexSchema());
        return snowflakeUtils.isTableExists(vertexConn, tableName, "VERTEX");
    }


    private void createVertexTables(List<S3Object> vertexFiles, SnowflakeProperties snowflakeProperties, Connection vertexConn) throws IOException {
        Map<String, List<JsonNode>> fileNameJsonObjectsMap = getJsonObjectsFromFilesWithSameName(vertexFiles, "nodes/");
        vertexTableCreationExecutor = new ThreadPoolExecutor(50,
                50, 60, TimeUnit.SECONDS, vertexTableCreationQueue);
        List<Future<?>> futureResp = new ArrayList<>();
        for (Map.Entry<String, List<JsonNode>> entry : fileNameJsonObjectsMap.entrySet()) {
            futureResp.add(vertexTableCreationExecutor.submit(() -> {
                log.info("S3ToSnowflake.createVertexTables(): file name = {}, json objects size = {}", entry.getKey(), entry.getValue().size());
                List<JsonNode> jsonObjects = entry.getValue();
                String[] columns = getColumnsToBeCreated(jsonObjects);
                if (snowflakeUtils.isTableExists(vertexConn, entry.getKey(), snowflakeProperties.getVertexSchema())) {
                    log.info("S3ToSnowflake.createVertexTables(): Table already exists. tableName={}", entry.getKey());
                } else {
                    snowflakeUtils.createTable(vertexConn, entry.getKey(), columns);
                }
                insertRecordsIntoVertexTable(jsonObjects, columns, vertexConn, entry.getKey());
            }));
        }

        for (Future<?> resp : futureResp) {
            try {
                resp.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("S3ToSnowflake.createVertexTables(): Error while creating vertex tables", e);
            }
        }
        log.info("S3ToSnowflake.createVertexTables(): Creation and Insertion of vertex tables is completed");
        vertexTableCreationExecutor.shutdown();

    }


    private Map<String, List<JsonNode>> getJsonObjectsFromFilesWithSameName(List<S3Object> vertexFiles, String folderName) {
        jsonObjectsExecutor = new ThreadPoolExecutor(100,
                100, 60, TimeUnit.SECONDS, jsonObjectsQueue);
        Map<String, List<JsonNode>> fileNameJsonObjectsMap = new ConcurrentHashMap<>();
        List<Future<?>> response = new ArrayList<>();
        for (S3Object file : vertexFiles) {
            response.add(jsonObjectsExecutor.submit(() -> {
                String fileName = file.key().split(folderName)[1].split("-")[0];
                if (fileNameJsonObjectsMap.containsKey(fileName)) {
                    List<JsonNode> existingNodes = fileNameJsonObjectsMap.get(fileName);
                    existingNodes.addAll(s3Utils.getFileContent(file.key()));
                    fileNameJsonObjectsMap.put(fileName, existingNodes);
                } else {
                    fileNameJsonObjectsMap.put(fileName, s3Utils.getFileContent(file.key()));
                }
            }));
        }

        for (Future<?> resp : response) {
            try {
                resp.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("getJsonObjectsFromFilesWithSameName(): Error while getting json objects from file", e);
            }
        }
        jsonObjectsExecutor.shutdown();
        return fileNameJsonObjectsMap;
    }

    private void insertRecordsIntoVertexTable(List<JsonNode> jsonObjects, String[] columns, Connection connection, String tableName) {
        List<String> ids = new ArrayList<>();
        try {

            jsonObjects.stream().forEach(jsonNode -> {
                String[] finalColumns = columns;
                String[] values = jsonUtils.parseJsonValues(jsonNode);
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].equals("id")) {
                        ids.add(values[i]);
                        break;
                    }
                }
                if (jsonObjects.indexOf(jsonNode) < 100) {
                    snowflakeUtils.insertData(connection, tableName, finalColumns, values);
                }
            });

        } catch (Exception e) {
            log.error("S3ToSnowflake.insertRecordsIntoTable(): Error while inserting record into table={}", tableName, e);
        }
        if (tableNameVertexIdsMap.containsKey(tableName)) {
            List<String> addedIds = tableNameVertexIdsMap.get(tableName);
            addedIds.addAll(ids);
            tableNameVertexIdsMap.put(tableName, addedIds);
        } else {
            tableNameVertexIdsMap.put(tableName, ids);
        }
        log.info("S3ToSnowflake.insertRecordsIntoTable(): Inserting data into table ={} is completed", tableName);
    }

    private void insertRecordsIntoEdgeTable(List<JsonNode> jsonObjects, String[] columns, Connection connection, String tableName) {
        if (jsonObjects.size() > 100) {
            jsonObjects = jsonObjects.subList(0, 100);
        }
        jsonObjects.stream().forEach(jsonNode -> {
            String[] finalColumns = columns;
            try {
                String[] values = jsonUtils.parseJsonValues(jsonNode);
                snowflakeUtils.insertData(connection, tableName, finalColumns, values);
            } catch (Exception e) {
                log.error("S3ToSnowflake.insertRecordsIntoTable(): Error while inserting record into table={}", tableName, e);
            }
        });
        log.info("S3ToSnowflake.insertRecordsIntoTable(): Inserting data into table ={} is completed", tableName);
    }


    private void createEdgeTables(List<S3Object> edgeFiles, SnowflakeProperties snowflakeProperties, Connection edgeConn) throws IOException {
        Map<String, List<JsonNode>> fileNameJsonObjectsMap = getJsonObjectsFromFilesWithSameName(edgeFiles, "edges/");
        edgeTableCreationExecutor = new ThreadPoolExecutor(50,
                50, 60, TimeUnit.SECONDS, edgeTableCreationQueue);
        List<Future<?>> futureResp = new ArrayList<>();
        for (Map.Entry<String, List<JsonNode>> entry : fileNameJsonObjectsMap.entrySet()) {
            futureResp.add(edgeTableCreationExecutor.submit(() -> {
                log.info("S3ToSnowflake.createEdgeTables(): file name = {}, json objects size = {}", entry.getKey(), entry.getValue().size());
                List<JsonNode> jsonObjects = entry.getValue();
                String[] columns = Arrays.stream(getColumnsToBeCreated(jsonObjects))
                        .map(column -> column.equalsIgnoreCase("from") ? "fromVertex" : column)
                        .map(column -> column.equalsIgnoreCase("to") ? "toVertex" : column)
                        .toArray(String[]::new);
                String[] values = jsonUtils.parseJsonValues(jsonObjects.get(0));
                String fromVertex = "";
                String toVertex = "";
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].equalsIgnoreCase("fromVertex")) {
                        fromVertex = values[i];
                    } else if (columns[i].equalsIgnoreCase("toVertex")) {
                        toVertex = values[i];
                    }
                }
                String fromVertexRefTable = getVertexRefTable(fromVertex);
                String toVertexRefTable = getVertexRefTable(toVertex);
                log.info("S3ToSnowflake.createEdgeTables(): edge table ={}, fromVertexRefTable={},toVertexRefTable={}", fromVertexRefTable, toVertexRefTable);
                if (snowflakeUtils.isTableExists(edgeConn, entry.getKey(), snowflakeProperties.getVertexSchema())) {
                    log.info("S3ToSnowflake.createEdgeTables(): Table already exists. tableName={}", entry.getKey());
                } else {
                    snowflakeUtils.createEdgeTable(edgeConn, entry.getKey(), columns, fromVertexRefTable, toVertexRefTable);
                }
                insertRecordsIntoEdgeTable(jsonObjects, columns, edgeConn, entry.getKey());
            }));
        }

        for (Future<?> resp : futureResp) {
            try {
                resp.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("S3ToSnowflake.createEdgeTables(): Error while creating edge tables", e);
            }
        }
        log.info("S3ToSnowflake.createEdgeTables(): Creation and Insertion of edge tables is completed");
        edgeTableCreationExecutor.shutdown();
    }

    private String getVertexRefTable(String vertexValue) {
        Optional<String> tableName = tableNameVertexIdsMap.entrySet().stream()
                .filter(entry -> entry.getValue().contains(vertexValue))
                .map(Map.Entry::getKey)
                .findFirst();

        return tableName.isPresent() ? tableName.get() : "";
    }

    private String getVertexRefTable(String vertexValue, Map<String,List<String>> vertexIdsMap) {
        Optional<String> tableName = vertexIdsMap.entrySet().stream()
                .filter(entry -> entry.getValue().contains(vertexValue))
                .map(Map.Entry::getKey)
                .findFirst();

        return tableName.isPresent() ? tableName.get() : "";
    }

    private String[] getColumnsToBeCreated(List<JsonNode> jsonObjects) {
        List<String[]> columnsArray = new ArrayList<>();
        jsonObjects.forEach(jsonNode -> {
            try {
                columnsArray.add(jsonUtils.parseJsonColumns(jsonNode));
            } catch (IOException e) {
                log.error("Error while preparing columns array list");
            }
        });

        String[] coulmnsToBeCreated = columnsArray.stream()
                .max(Comparator.comparingInt(arr -> arr.length))
                .orElse(null);
        return coulmnsToBeCreated;
    }


    public Map<String,Map<String,String>> getRefTables(){
        s3Utils.getS3Client();
        Map<String, List<String>> vertexIdsMap = getVertexIds();
        Map<String,Map<String,String>> respone = new ConcurrentHashMap<>();
        List<S3Object> edgeFiles = s3Utils.listFiles("edges");
        log.info("S3ToSnowflake.exportS3ObjectsToMySQL(): edges files size={}", edgeFiles.size());
        Map<String, List<JsonNode>> fileNameJsonObjectsMap = getJsonObjectsFromFilesWithSameName(edgeFiles, "edges/");

        edgeTableCreationExecutor = new ThreadPoolExecutor(50,
                50, 60, TimeUnit.SECONDS, edgeTableCreationQueue);
        List<Future<?>> futureResp = new ArrayList<>();
        for (Map.Entry<String, List<JsonNode>> entry : fileNameJsonObjectsMap.entrySet()) {
            futureResp.add(edgeTableCreationExecutor.submit(() -> {
                log.info("S3ToSnowflake.createEdgeTables(): file name = {}, json objects size = {}", entry.getKey(), entry.getValue().size());
                List<JsonNode> jsonObjects = entry.getValue();
                String[] columns = Arrays.stream(getColumnsToBeCreated(jsonObjects))
                        .map(column -> column.equalsIgnoreCase("from") ? "fromVertex" : column)
                        .map(column -> column.equalsIgnoreCase("to") ? "toVertex" : column)
                        .toArray(String[]::new);
                String[] values = jsonUtils.parseJsonValues(jsonObjects.get(0));
                String fromVertex = "";
                String toVertex = "";
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].equalsIgnoreCase("fromVertex")) {
                        fromVertex = values[i];
                    } else if (columns[i].equalsIgnoreCase("toVertex")) {
                        toVertex = values[i];
                    }
                }
                Map<String,String> map = new HashMap<>();
                String fromVertexRefTable = getVertexRefTable(fromVertex,vertexIdsMap);
                String toVertexRefTable = getVertexRefTable(toVertex,vertexIdsMap);
                map.put("fromVertexRefTable",fromVertexRefTable);
                map.put("toVertexRefTable",toVertexRefTable);
                respone.put(entry.getKey(),map);
                log.info("S3ToSnowflake.createEdgeTables(): edge table ={}, fromVertexRefTable={},toVertexRefTable={}", fromVertexRefTable, toVertexRefTable);
            }));
        }
        return respone;
    }

    public Map<String, List<String>> getVertexIds() {
        Map<String, Map<String, String>> response = new HashMap<>();
        Map<String, List<String>> vertexIdsMap = new HashMap<>();
        //s3Utils.getS3Client();
        // List files from vertex and edges folders
        List<S3Object> vertexFiles = s3Utils.listFiles("nodes");
        log.info("S3ToSnowflake.exportS3ObjectsToMySQL(): vertex files size ={}", vertexFiles.size());
        Map<String, List<JsonNode>> fileNameJsonObjectsMap = getJsonObjectsFromFilesWithSameName(vertexFiles, "nodes/");
        List<Future<?>> futureResp = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        vertexTableCreationExecutor = new ThreadPoolExecutor(50,
                50, 60, TimeUnit.SECONDS, vertexTableCreationQueue);
        for (Map.Entry<String, List<JsonNode>> entry : fileNameJsonObjectsMap.entrySet()) {
            String vertexFileName = entry.getKey();
            futureResp.add(vertexTableCreationExecutor.submit(() -> {
                log.info("S3ToSnowflake.createVertexTables(): file name = {}, json objects size = {}", entry.getKey(), entry.getValue().size());
                List<JsonNode> jsonObjects = entry.getValue();
                String[] columns = getColumnsToBeCreated(jsonObjects);

                jsonObjects.stream().forEach(jsonNode -> {
                    String[] finalColumns = columns;
                    String[] values = jsonUtils.parseJsonValues(jsonNode);
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i].equals("id")) {
                            ids.add(values[i]);
                            break;
                        }
                    }

                });
            }));
            if (vertexIdsMap.containsKey(vertexFileName)) {
                List<String> addedIds = vertexIdsMap.get(vertexFileName);
                addedIds.addAll(ids);
                vertexIdsMap.put(vertexFileName, addedIds);
            } else {
                vertexIdsMap.put(vertexFileName, ids);
            }

        }
        vertexTableCreationExecutor.shutdown();
        return vertexIdsMap;


    }
}
