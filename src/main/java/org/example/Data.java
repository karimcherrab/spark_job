package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

public class Data {

    String url_get_jobs = "http://localhost:3000/jobs/active";

    public Data() {
    }

    List<Map<String, Object>> get_jobs(){
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url_get_jobs))
                .build();

        // Send HTTP request asynchronously
        HttpResponse<String> httpResponse = null;
        try {
            httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Extract response body
        String responseBody = httpResponse.body();
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> maps = null;
        try {
            maps = objectMapper.readValue(responseBody, new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>(){});


        } catch (Exception e) {
            e.printStackTrace();
        }


        return maps;


    }
}
