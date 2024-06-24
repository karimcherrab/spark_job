package org.example;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CsvAppender {

    public CsvAppender() {
    }

    /**
     * Appends a row of data to the end of a CSV file, adding a header if the file is empty.
     *
     * @param filePath the path of the CSV file
     * @param header   the header row to write if the file is empty
     * @param data     the data to append (each element in the array is a column)
     */

    public static void appendToCsv(String filePath, String[] header, String[] data) {
        BufferedWriter writer = null;
        try {
            File file = new File(filePath);
            boolean fileExists = file.exists();
            boolean isNewFile = !fileExists || file.length() == 0;

            // Open the file in append mode
            writer = new BufferedWriter(new FileWriter(filePath, true));
            StringBuilder sb = new StringBuilder();

            // Write the header if the file is new or empty
            if (isNewFile) {
                for (int i = 0; i < header.length; i++) {
                    sb.append(header[i]);
                    if (i < header.length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
            }

            // Write the data row
            for (int i = 0; i < data.length; i++) {
                sb.append(data[i]);
                if (i < data.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");

            writer.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


}
