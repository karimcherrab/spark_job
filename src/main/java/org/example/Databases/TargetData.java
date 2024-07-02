package org.example.Databases;

import java.util.ArrayList;

public class TargetData {
    String database_name , table_name ;
    ArrayList<String> list_columns ;

    public TargetData(String database_name, String table_name, ArrayList<String> list_columns) {
        this.database_name = database_name;
        this.table_name = table_name;
        this.list_columns = list_columns;
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

    public ArrayList<String> getList_columns() {
        return list_columns;
    }

    public void setList_columns(ArrayList<String> list_columns) {
        this.list_columns = list_columns;
    }
}
