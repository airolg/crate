/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class CSVFileProcessor {
    private static final Logger logger = Loggers.getLogger(CSVFileProcessor.class);

    private static final byte NEW_LINE = (byte) '\n';
    private int recordsWritten;

    XContentBuilder builder;
    BufferedReader sourceReader;
    OutputStream outputStream;

    public CSVFileProcessor(BufferedReader sourceReader, OutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.sourceReader = sourceReader;
        this.recordsWritten = 0;
        builder = jsonBuilder(this.outputStream);
    }

    public void processToStream() throws IOException {
        if (isFileEmpty(sourceReader)) {
            logger.debug("An empty file has been input");
            return;
        }

        CSVParser csvParser = new CSVParser(sourceReader, CSVFormat.DEFAULT.withFirstRecordAsHeader());


        final Set<String> keys = csvParser.getHeaderMap().keySet();

        List<CSVRecord> csvRecords = csvParser.getRecords();

        convertCSVToJsonAndWriteObject(keys, csvRecords);
        csvParser.close();
    }

    private boolean isFileEmpty(BufferedReader reader) throws IOException {
        return !reader.ready();
    }

    private void convertCSVToJsonAndWriteObject(Set<String> keys, List<CSVRecord> csvRecords) throws IOException {
        for (CSVRecord record : csvRecords) {
            if (record.size() > keys.size()) throw new IllegalArgumentException("Number of row entries exceeds number of columns");

            writeObject(keys, record);
            recordsWritten++;
        }
    }

    private void writeObject(Set<String> keys, CSVRecord csvRecord) throws IOException {
        builder.startObject();
        keys.forEach(key -> {
            try {
                builder.field(key,csvRecord.get(key));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        builder.endObject();
        builder.flush();
        outputStream.write(NEW_LINE);
    }

    public int getRecordsWritten() {
        return recordsWritten;
    }
}

