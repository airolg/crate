package io.crate.operation.collect.files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CSVLineParser {

    public String parse(byte[] header, byte[] row) throws IOException {

        CSVParser headerParser = CSVFormat
            .DEFAULT
            .withFirstRecordAsHeader()
            .parse(new InputStreamReader(new ByteArrayInputStream(header), StandardCharsets.UTF_8));

        CSVParser rowParser = CSVFormat
            .DEFAULT
            .parse(new InputStreamReader(new ByteArrayInputStream(row), StandardCharsets.UTF_8));

        final Set<String> keys = headerParser.getHeaderMap().keySet();

        String parsedCsv = convertCSVToJsonString(keys, rowParser);

        headerParser.close();
        rowParser.close();
        return parsedCsv;
    }

    private String convertCSVToJsonString(Set<String> keys, CSVParser rowParser) {
        JSONObject convertedCsvToJson = new JSONObject();

        for (CSVRecord rowEntries : rowParser) {
            if (rowEntries.size() > keys.size())
                throw new IllegalArgumentException("Number of row entries exceeds number of columns");
            List<String> keyList = new ArrayList<>(keys);
            convertedCsvToJson = convertCSVToJsonObject(keyList, rowEntries);
        }

        return convertedCsvToJson.toString();
    }

    private JSONObject convertCSVToJsonObject(List<String> keys, CSVRecord rowEntries) {
        Map<String, String> mapForSingleRow = getMapOfKeysAndRowEntries(keys, rowEntries);
        return new JSONObject(mapForSingleRow);
    }

    private Map<String, String> getMapOfKeysAndRowEntries(List<String> keys, CSVRecord rowEntries) {
        return IntStream.range(0, keys.size())
            .boxed()
            .collect(Collectors.toMap(keys::get, rowEntries::get));
    }
}
