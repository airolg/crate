package io.crate.operation.collect.files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CSVFileProcessorTest {

    CSVFileProcessor subjectUnderTest;

    private static final byte NEW_LINE = (byte) '\n';

    private static File tmpFile;
    private static File tmpFileDuplicateKey;
    private static File tmpFileEmptyLine;
    private static File tmpFileEscapedChars;
    private static File tmpFileMissingKey;
    private static File tmpFileNoValues;
    private static File tmpFileMissingValue;
    private static File tmpFileExtraValue;
    private static File tmpFileLineBreakInValue;

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    private int records;
    private InputStream inputStream;

    @Mock
    private BufferedReader sourceReader;

    @Mock
    private ByteArrayOutputStreamWithExposedBuffer outputStream;


    ByteArrayInputStream resultInputStream;
    BufferedReader resultReader;
    CSVParser csvParser;

    @Before
    public void setup() throws IOException {
        subjectUnderTest = new CSVFileProcessor(sourceReader, outputStream);

        Path copy_from = createTempDirectory("copy_from");
        Path copy_from_escaped_chars = createTempDirectory("copy_from_duplicate_header");
        Path copy_from_empty = createTempDirectory("copy_from_empty");
        Path copy_from_duplicate_key = createTempDirectory("copy_from_duplicate_key");
        Path copy_from_missing_key = createTempDirectory("copy_from_missing_key");
        Path copy_from_no_values = createTempDirectory("copy_from_no_values");
        Path copy_from_missing_value = createTempDirectory("copy_from_missing_value");
        Path copy_from_extra_value = createTempDirectory("copy_from_extra_value");
        Path copy_from_line_break = createTempDirectory("copy_from_line_break");


        tmpFile = createTempFile(copy_from);
        tmpFileEmptyLine = createTempFile(copy_from_empty);
        tmpFileEscapedChars = createTempFile(copy_from_escaped_chars);
        tmpFileDuplicateKey = createTempFile(copy_from_duplicate_key);
        tmpFileMissingKey = createTempFile(copy_from_missing_key);
        tmpFileNoValues = createTempFile(copy_from_no_values);
        tmpFileMissingValue = createTempFile(copy_from_missing_value);
        tmpFileExtraValue = createTempFile(copy_from_extra_value);
        tmpFileLineBreakInValue = createTempFile(copy_from_line_break);


        createOutputStreamWriter(tmpFile, "Code,Country\n", "GER,Germany\n", "IRL,Ireland\n");
        createOutputStreamWriter(tmpFileEmptyLine, "Code,Country\n", "\n", "GER,Germany\n");
        createOutputStreamWriter(tmpFileEscapedChars, "Code,\"Coun, try\"\n", "GER,Germany\n");
        createOutputStreamWriter(tmpFileDuplicateKey, "Code,Country,Country\n", "GER,Germany\n");
        createOutputStreamWriter(tmpFileMissingKey, "Code,,Country\n", "GER,Germany\n");
        createOutputStreamWriter(tmpFileNoValues, "Code,,Country\n", "");
        createOutputStreamWriter(tmpFileMissingValue, "Code,Country\n", "GER,\n");
        createOutputStreamWriter(tmpFileExtraValue, "Code,Country\n", "GER,Germany,Another\n");
        createOutputStreamWriter(tmpFileLineBreakInValue, "Country\n", "\"Germ\nany\"\n");

    }

    @After
    public void teardown() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }

        deleteTmpFiles();
        assertDeletedTempFiles();
    }

    @Test
    public void CSVFileProcessor_givenCSVInput_thenProcessesFile_andConvertsToJson() throws IOException {
        givenInputStreamReceives(tmpFile);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenResultStartsWith("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
        thenResultStartsWith(" {\"Code\":\"IRL\",\"Country\":\"Ireland\"}");
        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    @Test
    public void processToStream_givenFileIsEmpty_thenSkipsFile() throws IOException {
        givenFileIsEmpty();

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_givenEmptyRow_andTheFileHasMoreThanOneRowOfValues_thenIgnoresEmptyRow() throws IOException {
        givenInputStreamReceives(tmpFileEmptyLine);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenResultStartsWith("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
        thenNumberOfRecordsWrittenIs(1);
    }

    @Test
    public void processToStream_givenEscapedCommaInRow_thenProcessesFileCorrectly() throws IOException {
        givenInputStreamReceives(tmpFileEscapedChars);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenResultStartsWith("{\"Code\":\"GER\",\"Coun, try\":\"Germany\"}");
        thenNumberOfRecordsWrittenIs(1);
        thenWritesToOutputStreamTimes(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void processToStream_givenDuplicateKey_thenThrowException() throws IOException {
        givenInputStreamReceives(tmpFileDuplicateKey);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
    }

    @Test(expected = IllegalArgumentException.class)
    public void processToStream_givenMissingKey_thenThrowException() throws IOException {
        givenInputStreamReceives(tmpFileMissingKey);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
    }

    @Test
    public void processToStream_givenValidRowOfKeys_andNoRowOfValues_thenSkipsFile() throws IOException {
        givenInputStreamReceives(tmpFileNoValues);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_givenRowWithMissingValue_thenTheValueIsAssignedToKeyAsAnEmptyString() throws IOException {
        givenInputStreamReceives(tmpFileMissingValue);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenResultStartsWith("{\"Code\":\"GER\",\"Country\":\"\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void processToStream_givenRowWithExtraValue_andTheFileHasOneRowOfValues_thenThrowsException() throws IOException {
        givenInputStreamReceives(tmpFileExtraValue);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
    }

    @Test
    public void processToStream_givenLineBreakInAValue_AndTheValueIsQuoted_thenProcessesFile() throws IOException {
        givenInputStreamReceives(tmpFileLineBreakInValue);
        givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
        givenByteArrayOutputStreamWithSize(209700000);
        givenCSVFileParserWithFormat(CSV_FORMAT);

        whenCSVFileProcessorIsInitialised();
        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
        givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

        thenNumberOfRecordsWrittenIs(1);
        thenWritesToOutputStreamTimes(1);
    }

    private Path createTempDirectory(String copy_from_no_values) throws IOException {
        return Files.createTempDirectory(copy_from_no_values);
    }

    private File createTempFile(Path copy_from) throws IOException {
        return File.createTempFile("csvFileProcessor", ".csv", copy_from.toFile());
    }

    private void createOutputStreamWriter(File tmpFileMissingKey, String header, String firstRow) throws IOException {
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFileMissingKey), StandardCharsets.UTF_8)) {
            writer.write(header);
            writer.write(firstRow);
        }
    }

    private void createOutputStreamWriter(File file, String header, String firstRow, String secondRow) throws IOException {
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
            writer.write(header);
            writer.write(firstRow);
            writer.write(secondRow);
        }
    }

    private void deleteTmpFiles() {
        tmpFile.deleteOnExit();
        tmpFileEmptyLine.deleteOnExit();
        tmpFileEscapedChars.deleteOnExit();
        tmpFileDuplicateKey.deleteOnExit();
        tmpFileMissingKey.deleteOnExit();
        tmpFileNoValues.deleteOnExit();
        tmpFileMissingValue.deleteOnExit();
        tmpFileExtraValue.deleteOnExit();
        tmpFileLineBreakInValue.deleteOnExit();
    }

    private void assertDeletedTempFiles() {
        assertThat(tmpFile.delete(), is(true));
        assertThat(tmpFileEmptyLine.delete(), is(true));
        assertThat(tmpFileEscapedChars.delete(), is(true));
        assertThat(tmpFileDuplicateKey.delete(), is(true));
        assertThat(tmpFileMissingKey.delete(), is(true));
        assertThat(tmpFileNoValues.delete(), is(true));
        assertThat(tmpFileMissingValue.delete(), is(true));
        assertThat(tmpFileExtraValue.delete(), is(true));
        assertThat(tmpFileLineBreakInValue.delete(), is(true));
    }

    private void givenFileIsEmpty() throws IOException {
        when(sourceReader.ready()).thenReturn(false);
    }

    private void givenInputStreamReceives(File exampleFile) throws IOException {
        inputStream = new FileInputStream(exampleFile.toString());
    }

    private void givenBufferedReaderWithInputStreamAndCharset(InputStream inputStream, Charset charset) {
        sourceReader = new BufferedReader(new InputStreamReader(inputStream, charset));
    }

    private void givenCSVFileParserWithFormat(CSVFormat csvFormat) throws IOException {
        csvParser = new CSVParser(sourceReader, csvFormat);
    }

    private void givenBufferedReaderWithResultInputStreamAndCharset(ByteArrayInputStream inputStream, Charset charset) {
        resultReader = new BufferedReader(new InputStreamReader(inputStream, charset));
    }

    private ByteArrayInputStream givenByteArrayInputStreamIsInitialisedWith(byte[] buffer) {
        return resultInputStream = new ByteArrayInputStream(buffer);
    }

    private void givenByteArrayOutputStreamWithSize(int size) {
        outputStream = spy(new ByteArrayOutputStreamWithExposedBuffer(size));
    }

    private void whenCSVFileProcessorIsInitialised() throws IOException {
        subjectUnderTest = new CSVFileProcessor(sourceReader, outputStream);
    }

    private void whenProcessToStreamIsCalled() throws IOException {
        subjectUnderTest.processToStream();
    }

    private void whenGetRecordsIsCalled() {
        records = subjectUnderTest.getRecordsWritten();
    }

    private void thenDoesNotWriteToOutputStream() {
        verify(outputStream, times(0)).write(NEW_LINE);
    }

    private void thenResultStartsWith(String expected) throws IOException {
        assertThat(resultReader.readLine(), is(expected));
    }

    private void thenNumberOfRecordsWrittenIs(int numberOfRecordsWritten) {
        assertThat(records, is(numberOfRecordsWritten));
    }

    private void thenWritesToOutputStreamTimes(int numberOfTimesOutput) {
        verify(outputStream, times(numberOfTimesOutput)).write(NEW_LINE);
    }

    class ByteArrayOutputStreamWithExposedBuffer extends ByteArrayOutputStream {

        public ByteArrayOutputStreamWithExposedBuffer(int size) {
            super(size);
        }

        public byte[] getBuffer() {
            return buf;
        }

    }
}
