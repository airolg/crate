package io.crate.operation.collect.files;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.io.*;
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
            logger.info("Got " + csvFileProcessor.getRecordsWritten() + "records written and " + csvFileProcessor.getSkipped() + "skipped");
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
        //this should be questioned
    }
}
