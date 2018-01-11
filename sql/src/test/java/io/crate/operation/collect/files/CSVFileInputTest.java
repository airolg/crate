package io.crate.operation.collect.files;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class CSVFileInputTest {
    private CSVFileInput subjectUnderTest;

    private static final URI TEST_URI = URI.create("file:///copy/test_copy_from.csv");
    private Predicate<URI> testPredicateURI = Predicate.isEqual(true);


    @Before
    public void setup() {
        subjectUnderTest = new CSVFileInput();
    }

    @Test
    public void listUris_givenURI_thenReturnsListContainingURI() {
        List<URI> result = subjectUnderTest.listUris(TEST_URI, testPredicateURI);

        assertThat(result, is(Collections.singletonList(TEST_URI)));
    }
}
