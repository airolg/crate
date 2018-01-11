package io.crate.operation.collect.files;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CSVFileProcessorTest {
    CSVFileProcessor subjectUnderTest;

    public static final String EXAMPLE_CSV_FILE = "/essetup/data/copy/test_copy_from.csv";
    private static final byte NEW_LINE = (byte) '\n';

    private int skipped;
    private int records;
    private InputStream inputStream;

    @Mock
    BufferedReader sourceReader;

    @Mock
    ByteArrayOutputStreamWithExposedBuffer outputStream;

    ByteArrayInputStream resultInputStream;
    BufferedReader resultReader;

    @Before
    public void setup() throws IOException {
        subjectUnderTest = new CSVFileProcessor(sourceReader, outputStream);
    }

    @Test
    public void CSVFileProcessor_givenCSVInput_thenProcessesFile_andConvertsToJson() throws IOException {
        givenInputStreamIsNull();
        try {
            givenInputStreamReceives(EXAMPLE_CSV_FILE);
            givenBufferedReaderWithInputStreamAndCharset(inputStream, StandardCharsets.UTF_8);
            givenByteArrayOutputStreamWithSize(209700000);

            whenCSVFileProcessorIsInitialised();
            whenProcessToStreamIsCalled();

            givenByteArrayInputStreamIsInitialisedWith(outputStream.getBuffer());
            givenBufferedReaderWithResultInputStreamAndCharset(resultInputStream, StandardCharsets.UTF_8);

            thenResultStartsWith("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    @Test
    public void processToStream_givenFileIsEmpty_thenSkipsFile() throws IOException {
        givenFileIsEmpty();

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_givenInvalidFirstLine_thenSkipsFile() throws IOException {
        givenFirstLineIs("Code,,Capital");

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_givenValidRowOfKeys_andNoRowOfValues_thenSkipsFile() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowOfValues(null);

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_givenRowWithMissingValue_andTheFileHasOneRowOfValues_thenSkipsFile() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowOfValues("GBR,Great Britain,");

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_and_getSkipped_givenRowWithMissingValues_andTheFileHasMoreThanOneRowOfValues_thenSkipsRow() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,Great Britain,London", "IRL,Ireland,");

        whenProcessToStreamIsCalled();
        whenGetSkippedIsCalled();

        thenNumberOfSkippedRowsIsEqualTo(1);
    }

    @Test
    public void processToStream_givenRowWithExtraValue_andTheFileHasOneRowOfValues_thenSkipsFile() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowOfValues("GBR,Great Britain,London,Another");

        whenProcessToStreamIsCalled();

        thenDoesNotWriteToOutputStream();
    }

    @Test
    public void processToStream_and_getSkipped_givenRowWithExtraValue_andTheFileHasMoreThanOneRowOfValues_thenSkipsRow() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,Great Britain,London,Another", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetSkippedIsCalled();

        thenNumberOfSkippedRowsIsEqualTo(1);
    }

    @Test
    public void processToStream_givenValidInput_thenProcessesFileCorrectly() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,Great Britain,London", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    @Test
    public void processToStream_givenRowSpreadsAcrossMultipleLines_thenProcessesFileCorrectly() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,Great Britain,\nLondon", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    @Test
    public void processToStream_givenEmptyRow_andTheFileHasMoreThanOneRowOfValues_thenIgnoresEmptyRow() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,Great Britain,London","", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    @Test
    public void processToStream_givenEscapedHeaderKey_thenProcessesFileCorrectly() throws IOException {
        givenFirstLineIs("Code,\"Country,\",Capital");
        givenRowsOfValuesAre("GBR,Great Britain,London", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    @Test
    public void processToStream_givenEscapedCommaInRow_thenProcessesFileCorrectly() throws IOException {
        givenFileHasValidFirstLineOfThreeKeys();
        givenRowsOfValuesAre("GBR,\"Great, Britain\",London", "IRL,Ireland,Dublin");

        whenProcessToStreamIsCalled();
        whenGetRecordsIsCalled();

        thenNumberOfRecordsWrittenIs(2);
        thenWritesToOutputStreamTimes(2);
    }

    private void givenFileIsEmpty() throws IOException {
        when(sourceReader.readLine()).thenReturn(null);
    }

    private void givenFileHasValidFirstLineOfThreeKeys() throws IOException {
        when(sourceReader.readLine()).thenReturn("Code,Country,Capital");
    }

    private void givenRowOfValues(String row) {
        when(sourceReader.lines()).thenReturn(Stream.of(row));
    }

    private void givenRowsOfValuesAre(String... rows) {
        when(sourceReader.lines()).thenReturn(Stream.of(rows));
    }

    private void givenInputStreamIsNull() {
        inputStream = null;
    }

    private void givenFirstLineIs(String input) throws IOException {
        when(sourceReader.readLine()).thenReturn(input);
    }
    private void givenInputStreamReceives(String exampleFile) throws IOException {
        inputStream = getClass().getResource(exampleFile).openStream();
    }
    private void givenBufferedReaderWithInputStreamAndCharset(InputStream inputStream, Charset charset) {
        sourceReader = new BufferedReader(new InputStreamReader(inputStream, charset));
    }

    private void givenBufferedReaderWithResultInputStreamAndCharset(ByteArrayInputStream inputStream, Charset charset) {
        resultReader = new BufferedReader(new InputStreamReader(inputStream, charset));
    }

    private ByteArrayInputStream givenByteArrayInputStreamIsInitialisedWith(byte[] buffer) {
        return resultInputStream = new ByteArrayInputStream(buffer);
    }

    private void givenByteArrayOutputStreamWithSize(int size) {
        outputStream = new ByteArrayOutputStreamWithExposedBuffer(size);
    }

    private void whenCSVFileProcessorIsInitialised() throws IOException {
        subjectUnderTest = new CSVFileProcessor(sourceReader, outputStream);
    }

    private void whenProcessToStreamIsCalled() throws IOException {
        subjectUnderTest.processToStream();
    }

    private void whenGetSkippedIsCalled() {
        skipped = subjectUnderTest.getSkipped();
    }

    private void whenGetRecordsIsCalled() {
        records = subjectUnderTest.getRecordsWritten();
    }
    private void thenNumberOfSkippedRowsIsEqualTo(int i) {
        assertThat(skipped, is(1));
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
