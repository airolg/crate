package io.crate.operation.collect.files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class CSVLineProcessor {

    private final OutputStream outputStream;
    private XContentBuilder builder;
    private static final byte NEW_LINE = (byte) '\n';

    public CSVLineProcessor(OutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        builder = jsonBuilder(this.outputStream);
    }

    public String parse(byte[] header, byte[] line) throws IOException {

        CSVParser headerParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new InputStreamReader(new ByteArrayInputStream(header), StandardCharsets.UTF_8));
        CSVParser lineParser = CSVFormat.DEFAULT.parse(new InputStreamReader(new ByteArrayInputStream(line), StandardCharsets.UTF_8));

        final Set<String> keys = headerParser.getHeaderMap().keySet();

        String json = convertCSVToJsonAndWriteObject(keys, lineParser);

        headerParser.close();
        lineParser.close();
        return json;
    }

    private String convertCSVToJsonAndWriteObject(Set<String> keys, CSVParser parser) {
        String json = "";
        for (CSVRecord record : parser) {
            if (record.size() > keys.size())
                throw new IllegalArgumentException("Number of row entries exceeds number of columns");
            List<String> keyList = new ArrayList<>();
            keyList.addAll(keys);
            json = convertCSVToJson(keyList, record);
        }
        return json;
    }

    private String convertCSVToJson(List<String> keys, CSVRecord values) {
        String json = "";

        Map<String, String> mapForSingleRow = getMapForSingleRow(keys, values);
        System.out.print(mapForSingleRow);
        try {
            json = writeObject(mapForSingleRow);
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        return new JSONObject(mapForSingleRow).toString();
    }

    private Map<String, String> getMapForSingleRow(List<String> keys, CSVRecord row) {
        return IntStream.range(0, keys.size())
            .boxed()
            .collect(Collectors.toMap(keys::get, row::get));
    }

    private String writeObject(Map<String, String> mapForSingleRow) throws IOException {
        builder.startObject();
        for (Map.Entry<String, String> mapEntry : mapForSingleRow.entrySet()) {
            builder.field(mapEntry.getKey(), mapEntry.getValue());
        }
        builder.endObject();
        builder.flush();
        System.out.print("json string = " + builder.toString());
        return builder.toString();
    }

}
