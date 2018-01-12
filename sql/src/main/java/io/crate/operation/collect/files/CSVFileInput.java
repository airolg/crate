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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class CSVFileInput implements FileInput {

    private static final Logger logger = Loggers.getLogger(CSVFileInput.class);

    @Override
    public List<URI> listUris(URI fileUri, Predicate<URI> uriPredicate) {
        return Collections.singletonList(fileUri);
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        InputStream inputStream = null;
        URL url = uri.toURL();

        if (!uri.toString().matches("\\*.csv")) {
            return null;
        }

        try {
            inputStream = url.openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(262144000);
            CSVFileProcessor csvFileProcessor = new CSVFileProcessor(reader, outputStream);
            csvFileProcessor.processToStream();
            byte[] csv = outputStream.toByteArray();
            outputStream.close();
            logger.info(csvFileProcessor.getRecordsWritten() + "records written");
            return new ByteArrayInputStream(csv);
        } catch (FileNotFoundException e) {
            return null;
        } finally {
            if (inputStream != null)
                inputStream.close();
        }
    }

    @Override
    public boolean sharedStorageDefault() {
        return false;
    }
}
