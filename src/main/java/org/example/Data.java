package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Data {
    String url_get_jobs = "http://localhost:3000/jobs/active";
    private static final String urlGetDatabaseTypes = "http://localhost:3000/bd_type";
    private static final String URL_POST_ALERT = "http://localhost:3000/alert";
    public Data() {
    }
    List<Map<String, Object>> get_jobs(){
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url_get_jobs))
                .build();
        HttpResponse<String> httpResponse = null;
        try {
            httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        String responseBody = httpResponse.body();
        System.out.println(responseBody);
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> maps = null;
        try {
            maps = objectMapper.readValue(responseBody, new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>(){});
        } catch (Exception e) {
            e.printStackTrace();
        }
        return maps;
    }








    public String postAlert(int id_job,String databaseType, String clientAddress, String database,  List<String>  tables,  List<String>  columns) {
        HttpClient client = HttpClient.newHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        System.out.println(tables + "//////////////" + columns);
        // Create JSON payload
        Map<String, Object> data = new HashMap<>();
        data.put("id_job", id_job);
        data.put("database_type", databaseType);
        data.put("client_address", clientAddress);
        data.put("database", database);
        data.put("tables", tables);
        data.put("columns", columns);

        String requestBody = null;
        try {
            requestBody = objectMapper.writeValueAsString(data);
        } catch (IOException e) {
            e.printStackTrace();
            return "Failed to convert data to JSON";
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(URL_POST_ALERT))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return "Failed to send request";
        }
    }
//
//    public static void main(String[] args) {
//        Data data = new Data();
//        String response = data.postAlert("PostgreSQL", "192.168.1.1", "testdb", "testtable", "testcolumn");
//        System.out.println("Response from server: " + response);
//    }
}
