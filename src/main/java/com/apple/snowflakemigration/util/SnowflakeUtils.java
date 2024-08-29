package com.apple.snowflakemigration.util;

import com.apple.snowflakemigration.model.SnowflakeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

@Component
public class SnowflakeUtils {


        private static Logger log = LoggerFactory.getLogger(SnowflakeUtils.class);

        private static final int batchSize = 100;


        public Connection getConnection(SnowflakeProperties snowflakeProps, String schema){
            Connection conn = null;
            Properties properties = new Properties();
            properties.put("user", snowflakeProps.getUsername());
            properties.put("password", snowflakeProps.getPassword());
            properties.put("account", snowflakeProps.getAccount());
            properties.put("db", snowflakeProps.getDb());
            properties.put("schema", schema);
            properties.put("role", snowflakeProps.getRole());
            try {
                 conn = DriverManager.getConnection(snowflakeProps.getConnectionUrl(), properties);
            } catch (SQLException e) {
               log.error("SnowflakeUtils.getConnection(), Error while getting connection",e);
            }
            return conn;
        }


        public boolean isTableExists(Connection connection, String tableName, String schema) {
        try {
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, schema, tableName, null);
            return tables.next();
        } catch (Exception e) {
            log.error("SnowflakeUtils.tableExists(): Error while checking whether table exists or not",tableName,e);
            return false;
        }
    }

        public void createTable(Connection connection,String tableName, String[] columns) {
        try (Statement stmt = connection.createStatement()) {
            StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + "(");
            for (String column : columns) {
                if(column.equalsIgnoreCase("id")){
                    sql.append(column).append(" VARCHAR(255) PRIMARY KEY, ");
                }else{
                    sql.append(column).append(" TEXT, ");
                }
            }
            sql.delete(sql.length() - 2, sql.length()).append(")");
            stmt.executeUpdate(sql.toString());
            log.info("SnowflakeUtils.createTable(): Created table={}",tableName);
        } catch (Exception e) {
            log.error("SnowflakeUtils.createTable(): Error while creating table={}",tableName,e);
        }
    }


       public void createEdgeTable(Connection connection,String tableName,String[] columns, String fromVertexRefTable, String toVertexRefTable) {
        try (Statement stmt = connection.createStatement()) {
            StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + "(");
            for (String column : columns) {
                if(column.equalsIgnoreCase("id")){
                    sql.append(column).append(" VARCHAR(255) PRIMARY KEY, ");
                }
                else{
                    sql.append(column).append(" TEXT, ");
                }
            }
            if(StringUtils.hasLength(fromVertexRefTable)){
                sql.append("FOREIGN KEY (fromVertex) REFERENCES "+fromVertexRefTable+"(id),");
            }
            if(StringUtils.hasLength(toVertexRefTable)){
                sql.append("FOREIGN KEY (toVertex) REFERENCES "+toVertexRefTable+"(id),");
            }

            // sql.append("FOREIGN KEY (to) REFERENCES vertex(id),");
            sql.delete(sql.length() - 2, sql.length()).append(")");
            stmt.executeUpdate(sql.toString());
            log.info("SnowflakeUtils.createEdgeTable(): Created table={}",tableName);
        } catch (Exception e) {
            log.error("SnowflakeUtils.createEdgeTable(): Error while creating table={}",tableName,e);
        }
    }



        public void insertData(Connection connection,String tableName, String[] columns, String[] values) {
            // Ensure values array length matches columns array length by adding nulls if necessary
            PreparedStatement pstmt = null;
            if (columns.length > values.length) {
                values = Arrays.copyOf(values, columns.length); // Expand values array
                for (int i = values.length - columns.length; i < values.length; i++) {
                    if (values[i] == null) {
                        values[i] = ""; // Replace nulls with empty strings
                    }
                }
            }
            StringBuilder placeholders = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                placeholders.append("?, ");
            }
            placeholders.delete(placeholders.length() - 2, placeholders.length());

            String sql = "INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" + placeholders.toString() + ")";

            try  {
                pstmt = connection.prepareStatement(sql);
                for (int i = 0; i < values.length; i++) {
                    pstmt.setString(i + 1, values[i]);
                }
                pstmt.executeUpdate();
                // pstmt.addBatch(); // Add the insert statement to the batch
            } catch (Exception e) {
                log.error("SnowflakeUtils.insertData(): Error while inserting data into table={}", tableName, e);
            }
            //return pstmt;
        }


}
