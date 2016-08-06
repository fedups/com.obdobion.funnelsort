package com.obdobion.funnel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringBufferInputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.provider.CsvProvider;

/**
 * @author Chris DeGreef
 *
 */
@SuppressWarnings("deprecation")
public class CsvTest
{

    static private String csvColumns = " --col(Int --field 1 -n Number)(String --field 2 -n A)(String --field 3 -n B) ";

    static private List<String> csvInput()
    {
        final List<String> in = new ArrayList<>();
        in.add("1,a,b");
        in.add("2,a,bb");
        in.add("3,a,bbb");
        in.add("4,a,bbbb");
        in.add("5,a,bbbbb");
        in.add("6,a,b");
        in.add("7,aa,b");
        in.add("8,aaa,b");
        in.add("9,aaaa,b");
        in.add("10,aaaaa,b");
        return in;
    }

    @Test
    public void csvAllDefault() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("abc,3");
        out.add("def,3");

        final StringBuilder sb = new StringBuilder();
        for (int in = 1; in >= 0; in--)
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(),
                "--csv() --row 2 "
                        + "--col(I, --fi 2 -n field1)(S --fi 1 -n field0) "
                        + "--orderBy (field1)(field0)");

        Assert.assertEquals("records", 2L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("def,3");
        out.add("abc,5");

        final StringBuilder sb = new StringBuilder();
        for (int in = 1; in >= 0; in--)
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(),
                "--csv() "
                        + "--col(String -f1 -nA)(Int -f2 -nB)"
                        + "--orderBy(A desc) --row 2 ");

        Assert.assertEquals("records", 2L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvHeader() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("letters,numbers");
        out.add("def,3");
        out.add("abc,5");

        final StringBuilder sb = new StringBuilder();
        sb.append(out.get(0)).append(System.getProperty("line.separator"));
        for (int in = 2; in >= 1; in--)
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(), "--csv(-h) "
                + "--col(String -f1 -nletters)(Int -f2 -nnumbers) "
                + "--orderBy(numbers) --row 2 ");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserCommentMarker() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--commentMarker ',') "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserDelimiter() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--delimiter ',') "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserEscape() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--escape '~') "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserIgnoreEmptyLines() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--ignoreEmptyLines) "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserIgnoreSurroundingSpaces() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--ignoreSurroundingSpaces) "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserNullString() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserNullString", in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--NullString 'n/a') "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserQuote() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile(testName, in);
        Funnel.sort(Helper.config(), file.getAbsolutePath() + " -r --csv(--quote \"'\") "
                + csvColumns
                + "--orderBy(Number desc)");

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void field1() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "field1,field2".getBytes();

        final CsvProvider csv = new CsvProvider(new boolean[] {
                true,
                false
        });

        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field1", new String(result[0]));
    }

    @Test
    public void field1TrimNeeded() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "\" \tfield1\t , \",field2".getBytes();
        final CsvProvider csv = new CsvProvider(new boolean[] {
                true,
                false
        });
        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", " \tfield1\t , ", new String(result[0]));
    }

    @Test
    public void field1WithQuotedComma() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "\"field1,\",field2".getBytes();
        final CsvProvider csv = new CsvProvider(new boolean[] {
                true,
                false
        });
        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field1,", new String(result[0]));
    }

    @Test
    public void field2() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "field1,field2".getBytes();
        final CsvProvider csv = new CsvProvider(new boolean[] {
                false,
                true
        });
        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field2", new String(result[1]));
    }

    @Test
    public void field2WithQuotedComma() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "\"field1,\",field2".getBytes();
        final CsvProvider csv = new CsvProvider(new boolean[] {
                false,
                true
        });
        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field2", new String(result[1]));
    }

    @Test
    public void keyCsvFields() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "--csv () "
                    + "--col(String -f1 -na)(Date -f2 -nd)"
                    + "--orderBy (a)(d)");
        } catch (final ParseException pe)
        {
            Assert.fail(pe.getMessage());
        }
    }

    @Test
    public void keyCsvFieldsOnNonCsvFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), csvColumns + "--orderBy (Number)(A)");
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected CSV key (--field) on a non-CSV file", pe.getMessage());
        }
    }

    @Test
    public void normalKeysOnCsvFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "--csv() --col(String -o0 -n Name) --orderBy(Name)");
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("only CSV keys (--field) allowed on a CSV file", pe.getMessage());
        }
    }

    @Test
    public void repeatedField() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "--csv() " + csvColumns + "--orderBy (A)(Number)(Number)");
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("sorting on the same field (--field 0) is not allowed", pe.getMessage());
        }
    }

    @Test
    public void sortField0Desc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("10,aaaaa,b");
        out.add("9,aaaa,b");
        out.add("8,aaa,b");
        out.add("7,aa,b");
        out.add("6,a,b");
        out.add("5,a,bbbbb");
        out.add("4,a,bbbb");
        out.add("3,a,bbb");
        out.add("2,a,bb");
        out.add("1,a,b");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(
                Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy(Number desc)");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField1Asc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("1,a,b");
        out.add("2,a,bb");
        out.add("3,a,bbb");
        out.add("4,a,bbbb");
        out.add("5,a,bbbbb");
        out.add("6,a,b");
        out.add("7,aa,b");
        out.add("8,aaa,b");
        out.add("9,aaaa,b");
        out.add("10,aaaaa,b");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy(A)");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField1Desc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("10,aaaaa,b");
        out.add("9,aaaa,b");
        out.add("8,aaa,b");
        out.add("7,aa,b");
        out.add("1,a,b");
        out.add("2,a,bb");
        out.add("3,a,bbb");
        out.add("4,a,bbbb");
        out.add("5,a,bbbbb");
        out.add("6,a,b");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy(A desc)");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField2Asc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("1,a,b");
        out.add("6,a,b");
        out.add("7,aa,b");
        out.add("8,aaa,b");
        out.add("9,aaaa,b");
        out.add("10,aaaaa,b");
        out.add("2,a,bb");
        out.add("3,a,bbb");
        out.add("4,a,bbbb");
        out.add("5,a,bbbbb");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy (B) ");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField2Desc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("5,a,bbbbb");
        out.add("4,a,bbbb");
        out.add("3,a,bbb");
        out.add("2,a,bb");
        out.add("1,a,b");
        out.add("6,a,b");
        out.add("7,aa,b");
        out.add("8,aaa,b");
        out.add("9,aaaa,b");
        out.add("10,aaaaa,b");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy(B desc)");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortOnTwoFieldsAsc() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = csvInput();

        final List<String> out = new ArrayList<>();
        out.add("10,aaaaa,b");
        out.add("9,aaaa,b");
        out.add("8,aaa,b");
        out.add("7,aa,b");
        out.add("6,a,b");
        out.add("1,a,b");
        out.add("2,a,bb");
        out.add("3,a,bbb");
        out.add("4,a,bbbb");
        out.add("5,a,bbbbb");

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file.getAbsolutePath() + " -r --csv() "
                        + csvColumns
                        + "--orderBy(B)(Number desc)");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void twoFields() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final byte[] csvData = "\" \tfield1\t , \",field2".getBytes();
        final CsvProvider csv = new CsvProvider(new boolean[] {
                true,
                true
        });
        final CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", " \tfield1\t , ", new String(result[0]));
        Assert.assertEquals("extract field 1", "field2", new String(result[1]));
    }
}
