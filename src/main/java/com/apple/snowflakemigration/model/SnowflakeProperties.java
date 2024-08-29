package com.apple.snowflakemigration.model;

public class SnowflakeProperties {

    private String connectionUrl;
    private String username;

    private String account;

    private String password;

    private String db;

    private String vertexSchema;
    private String edgeSchema;

    private String role;

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }


    public String getVertexSchema() {
        return vertexSchema;
    }

    public void setVertexSchema(String vertexSchema) {
        this.vertexSchema = vertexSchema;
    }

    public String getEdgeSchema() {
        return edgeSchema;
    }

    public void setEdgeSchema(String edgeSchema) {
        this.edgeSchema = edgeSchema;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}
