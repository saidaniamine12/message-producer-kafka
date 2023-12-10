package org.example;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLOutput;
import java.util.Properties;
import java.util.Random;

public class App {
    private static final String[] SELLERS = {"seller1", "seller2", "seller3"};

    public static class SaleEvent {
        private String sellerId;
        private double amountUsd;
        private long saleTimestamp;

        // Constructors, getters, setters...

        public String getSellerId() {
            return sellerId;
        }
        public void setSellerId(String sellerId) {
            this.sellerId = sellerId;
        }
        public Double getAmountUsd() {
            return amountUsd;
        }
        public void setAmountUsd(int amountUsd) {
            this.amountUsd = amountUsd;
        }
        public long getSaleTimestamp() {
            return saleTimestamp;
        }
        public void setSaleTimestamp(long saleTimestamp) {
            this.saleTimestamp = saleTimestamp;
        }

        public String toJson() {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.writeValueAsString(this);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        public String toString() {
            return "SaleEvent{" +
                    "sellerId='" + sellerId + '\'' +
                    ", amountUsd=" + amountUsd +
                    ", saleTimestamp=" + saleTimestamp +
                    '}';
        }
    }


    public static void main(String[] args) {

        //System.out.println(getDollarRateForTND());
        String bootstrapServers = "localhost:9092";
        String topic = "flink_input_topic";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 1;
        Random random = new Random();

        while (true) {
            boolean isTenth = i % 10 == 0;

            String sellerId = SELLERS[random.nextInt(SELLERS.length)];
            int amountUsd = random.nextInt(900) + 100;
            long saleTimestamp = System.currentTimeMillis();

            SaleEvent saleEvent = new SaleEvent();
            saleEvent.setSellerId(sellerId);
            saleEvent.setAmountUsd(amountUsd);
            saleEvent.setSaleTimestamp(saleTimestamp);

            // Convert SaleEvent object to JSON string
            String salesJson = saleEvent.toJson();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, salesJson);
            producer.send(record, (metadata, exception) -> {
                if (isTenth) {
                    if (exception == null) {
                        System.out.println("Message sent successfully: " + salesJson);
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

            if (isTenth) {
                producer.flush();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i = 0;
            }

            i++;
        }
    }

    public static Double getDollarRateForTND() {

        Double usdToDinarRate = null;

        try {
            // Replace with your actual API key and endpoint
            String apiKey = "1705a49c2d4a22a8ba698b07";
            String endpoint = "https://v6.exchangerate-api.com/v6/1705a49c2d4a22a8ba698b07/latest/USD";

            // Create URL object
            URL url = new URL(endpoint);

            // Open connection
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set request method
            connection.setRequestMethod("GET");

            // Get response code
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Read response
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                reader.close();

                // Parse the JSON response
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonResponse = objectMapper.readTree(response.toString());

                // Extract the USD to Dinar exchange rate
                usdToDinarRate = jsonResponse
                        .path("conversion_rates")
                        .path("TND")
                        .asDouble();

                System.out.println("USD to Dinar Exchange Rate: " + usdToDinarRate);

            } else {
                System.out.println("API request failed. Response Code: " + responseCode);

            }

            // Close connection
            connection.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return usdToDinarRate;
    }
}
