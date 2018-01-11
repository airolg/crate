package io.crate.operation.collect.files;


import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class CSVFileProcessor {
    private static final Logger logger = Loggers.getLogger(CSVFileProcessor.class);

    private static final byte NEW_LINE = (byte) '\n';
    private int skipped;
    private int recordsWritten;

    XContentBuilder builder;
    BufferedReader sourceReader;
    OutputStream outputStream;

    //Should this class be renamed e.g. to ReadingIterator? Or should it be aligned to it??

//    exact semantics of our json COPY FROM but since the CSV is just another file format, the semantics should be the same.
//    Empty rows can be skipped but missing header or duplicate keys in the header have to return an error to the user.
//    In the case of a line break within a value, the value itself has to be quoted

    public CSVFileProcessor(BufferedReader sourceReader, OutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.sourceReader = sourceReader;
        this.recordsWritten = 0;
        this.skipped = 0;
        builder = jsonBuilder(this.outputStream);
    }

    public void processToStream() throws IOException {
        final List<String> keys;
        final int numberOfKeys;
        final List<List<String>> listOfRows;

        final String fileContent = sourceReader.readLine();

        if (isFileEmpty(fileContent)) {
            logger.debug("An empty file has been input");
            return;
        }

        keys = extractColumnValues(fileContent);
        numberOfKeys = keys.size();

        if (emptyKeyPresent(keys)) {
            logger.error("One or more of the key entries was null or empty");
            return;
        }

        listOfRows = extractListOfRowsWithValues();

        if (isRowWithEmptyValuePresent(listOfRows, numberOfKeys)) {
            List<List<String>> listOfValidRows = listOfRowsWithInvalidRowsRemoved(listOfRows, numberOfKeys);
            skipped += listOfRows.size() - listOfValidRows.size();
            convertCSVToJson(keys, listOfValidRows);
        } else {
            convertCSVToJson(keys, listOfRows);
        }
    }

    public int getRecordsWritten() {
        return recordsWritten;
    }

    public int getSkipped() {
        return skipped;
    }

    private boolean isFileEmpty(String content) throws IOException {
        return content == null || content.isEmpty();
    }

    private boolean emptyKeyPresent(List<String> keys) {
        return keys.stream().anyMatch(key -> key == null || key.isEmpty());
    }

    private List<List<String>> extractListOfRowsWithValues() {
        return sourceReader.lines()
                .filter(row -> row != null && !row.isEmpty())
                .map(this::extractColumnValues)
                .collect(toList());
    }

    private List<String> extractColumnValues(String firstLine) {
        return Arrays.asList(firstLine.split(","));
    }

    private boolean isRowWithEmptyValuePresent(List<List<String>> rowsOfValues, int numberOfKeys) {
        return rowsOfValues
                .stream()
                .anyMatch(row -> row.size() != numberOfKeys);
    }

    private  List<List<String>> listOfRowsWithInvalidRowsRemoved(List<List<String>> listOfRows, int numberOfKeys) {
        return listOfRows.stream()
                .filter(row -> row.size() == numberOfKeys)
                .collect(toList());
    }

    private void convertCSVToJson(List<String> keys, List<List<String>> values) {
        values.forEach(row -> {
            Map<String, String> mapForSingleRow = getMapForSingleRow(keys, row);
            try {
                logger.debug("Entry: " + mapForSingleRow);
                writeObject(mapForSingleRow);
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        });
    }

    private Map<String, String> getMapForSingleRow(List<String> keys, List<String> row) {
        return IntStream.range(0, keys.size())
                .boxed()
                .collect(Collectors.toMap(
                        keys::get,
                        row::get,
                        (u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
                        LinkedHashMap::new));
        //Forcing it to use a LinkedHashMap this a good idea? -> all brings is testability
    }

    private void writeObject(Map<String, String> mapForSingleRow) throws IOException {
        builder.startObject();
        for (Map.Entry<String, String> mapEntry : mapForSingleRow.entrySet()) {
            builder.field(mapEntry.getKey(), mapEntry.getValue());
        }
        builder.endObject();
        builder.flush();
        outputStream.write(NEW_LINE);
        recordsWritten++;
    }
}

