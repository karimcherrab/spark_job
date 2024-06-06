package org.example;

import java.io.Serializable;
import java.util.ArrayList;

public class Job implements Serializable {
    String id , name , type_database , type_chrono ;


    String database_name , table_name;
    ArrayList<String> cols = new ArrayList<>();

    public Job(String id, String name, String type_database, String type_chrono, String database_name, String table_name, ArrayList<String> cols) {
        this.id = id;
        this.name = name;
        this.type_database = type_database;
        this.type_chrono = type_chrono;
        this.database_name = database_name;
        this.table_name = table_name;
        this.cols = cols;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType_database() {
        return type_database;
    }

    public void setType_database(String type_database) {
        this.type_database = type_database;
    }

    public String getType_chrono() {
        return type_chrono;
    }

    public void setType_chrono(String type_chrono) {
        this.type_chrono = type_chrono;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public ArrayList<String> getCols() {
        return cols;
    }

    public void setCols(ArrayList<String> cols) {
        this.cols = cols;
    }
}
