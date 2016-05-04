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
import com.obdobion.funnel.provider.VariableLengthCsvProvider;

/**
 * @author Chris DeGreef
 * 
 */
@SuppressWarnings("deprecation")
public class CsvTest
{

    static private String csvColumns = " --col(Int --field 0 -n Number)(String --field 1 -n A)(String --field 2 -n B) ";
    static private List<String> csvInput ()
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
    public void csvAllDefault () throws Throwable
    {
        Helper.initializeFor("TEST csvAllDefault");

        final List<String> out = new ArrayList<>();
        out.add("abc,3");
        out.add("def,3");

        final StringBuilder sb = new StringBuilder();
        for (int in = 1; in >= 0; in--)
        {
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("--csv() --max 2 --eol cr,lf "
                + "--col(I, --fi 1 -n field1)(S --fi 0 -n field0) "
                + "--orderBy (field1)(field0)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvFile () throws Throwable
    {
        Helper.initializeFor("TEST csvFile");

        final List<String> out = new ArrayList<>();
        out.add("def,3");
        out.add("abc,5");

        final StringBuilder sb = new StringBuilder();
        for (int in = 1; in >= 0; in--)
        {
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("--csv() "
                + "--col(String -f0 -nA)(Int -f1 -nB)"
                + "--orderBy(A desc) --max 2 --eol cr,lf "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvHeader () throws Throwable
    {
        Helper.initializeFor("TEST csvHeader");

        final List<String> out = new ArrayList<>();
        out.add("letters,numbers");
        out.add("def,3");
        out.add("abc,5");

        final StringBuilder sb = new StringBuilder();
        sb.append(out.get(0)).append(System.getProperty("line.separator"));
        for (int in = 2; in >= 1; in--)
        {
            sb.append(out.get(in)).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("--csv(-h) "
                + "--col(String -f0 -nletters)(Int -f1 -nnumbers) "
                + "--orderBy(numbers) --max 2 --eol cr,lf "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void field1 () throws Throwable
    {
        Helper.initializeFor("TEST field1");

        final byte[] csvData = "field1,field2".getBytes();

        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                true, false
        });

        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field1", new String(result[0]));
    }

    @Test
    public void field1TrimNeeded () throws Throwable
    {
        Helper.initializeFor("TEST field1TrimNeeded");

        final byte[] csvData = "\" \tfield1\t , \",field2".getBytes();
        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                true, false
        });
        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", " \tfield1\t , ", new String(result[0]));
    }

    @Test
    public void field1WithQuotedComma () throws Throwable
    {
        Helper.initializeFor("TEST field1WithQuotedComma");

        final byte[] csvData = "\"field1,\",field2".getBytes();
        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                true, false
        });
        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field1,", new String(result[0]));
    }

    @Test
    public void field2 () throws Throwable
    {
        Helper.initializeFor("TEST field2");

        final byte[] csvData = "field1,field2".getBytes();
        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                false, true
        });
        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field2", new String(result[1]));
    }

    @Test
    public void field2WithQuotedComma () throws Throwable
    {
        Helper.initializeFor("TEST field2WithQuotedComma");

        final byte[] csvData = "\"field1,\",field2".getBytes();
        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                false, true
        });
        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", "field2", new String(result[1]));
    }

    @Test
    public void keyCsvFields () throws Throwable
    {
        Helper.initializeFor("TEST keyCsvField");
        try
        {
            Funnel.sort("--csv () "
                    + "--col(String -f0 -na)(Date -f1 -nd)"
                    + "--orderBy (a)(d)"
                    + Helper.DEFAULT_OPTIONS);
        } catch (final ParseException pe)
        {
            Assert.fail(pe.getMessage());
        }
    }

    @Test
    public void keyCsvFieldsOnNonCsvFile () throws Throwable
    {
        Helper.initializeFor("TEST keyCsvFieldsOnNonCsvFile");
        try
        {
            Funnel.sort(csvColumns +
                    "--orderBy (Number)(A)"
                    + Helper.DEFAULT_OPTIONS);
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected CSV key (--field) on a non-CSV file", pe.getMessage());
        }
    }

    @Test
    public void normalKeysOnCsvFile () throws Throwable
    {
        Helper.initializeFor("TEST normalKeysOnCsvFile");
        try
        {
            Funnel.sort("--csv() --col(String -o0 -n Name) --orderBy(Name)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("only CSV keys (--field) allowed on a CSV file", pe.getMessage());
        }
    }

    @Test
    public void repeatedField () throws Throwable
    {
        Helper.initializeFor("TEST repeatedField");
        try
        {
            Funnel.sort("--csv() " + csvColumns + "--orderBy (A)(Number)(Number)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("Expected a ParseException");

        } catch (final ParseException pe)
        {
            Assert.assertEquals("sorting on the same field (--field 0) is not allowed", pe.getMessage());
        }
    }

    @Test
    public void csvParserCommentMarker () throws Throwable
    {

        Helper.initializeFor("TEST csvParserCommentMarker");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserCommentMarker", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--commentMarker ',') "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserDelimiter () throws Throwable
    {

        Helper.initializeFor("TEST csvParserDelimiter");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserDelimiter", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--delimiter ',') "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserEscape () throws Throwable
    {

        Helper.initializeFor("TEST csvParserEscape");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserEscape", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--escape '~') "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserIgnoreEmptyLines () throws Throwable
    {

        Helper.initializeFor("TEST csvParserIgnoreEmptyLines");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserIgnoreEmptyLines", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--ignoreEmptyLines) "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserNullString () throws Throwable
    {

        Helper.initializeFor("TEST csvParserNullString");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserNullString", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--NullString 'n/a') "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserQuote () throws Throwable
    {

        Helper.initializeFor("TEST csvParserQuote");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserQuote", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--quote \"'\") "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void csvParserIgnoreSurroundingSpaces () throws Throwable
    {

        Helper.initializeFor("TEST csvParserIgnoreSurroundingSpaces");

        final List<String> in = csvInput();

        final File file = Helper.createUnsortedFile("csvParserIgnoreSurroundingSpaces", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv(--ignoreSurroundingSpaces) "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField0Desc () throws Throwable
    {

        Helper.initializeFor("TEST sortField0Desc");

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

        final File file = Helper.createUnsortedFile("sortField0Desc", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv() "
                + csvColumns
                + "--orderBy(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField1Asc () throws Throwable
    {

        Helper.initializeFor("TEST sortField1Asc");

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

        final File file = Helper.createUnsortedFile("sortField1Asc", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv() "
                + csvColumns
                + "--orderBy(A)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField1Desc () throws Throwable
    {

        Helper.initializeFor("TEST sortField1Desc");

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

        final File file = Helper.createUnsortedFile("sortField1Desc", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv() "
                + csvColumns
                + "--orderBy(A desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField2Asc () throws Throwable
    {

        Helper.initializeFor("TEST sortField2Asc");

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

        final File file = Helper.createUnsortedFile("sortField2Asc", in);
        final FunnelContext context = Funnel.sort(
                file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                        + "--csv() "
                        + csvColumns
                        + "--orderBy (B) "
                        + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortField2Desc () throws Throwable
    {

        Helper.initializeFor("TEST sortField2Desc");

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

        final File file = Helper.createUnsortedFile("sortField2Desc", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv() "
                + csvColumns
                + "--orderBy(B desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortOnTwoFieldsAsc () throws Throwable
    {

        Helper.initializeFor("TEST sortOnTwoFieldsAsc");

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

        final File file = Helper.createUnsortedFile("sortOnTwoFieldsAsc", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --csv() "
                + csvColumns
                + "--orderBy(B)(Number desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void twoFields () throws Throwable
    {

        Helper.initializeFor("TEST twoFields");

        final byte[] csvData = "\" \tfield1\t , \",field2".getBytes();
        final VariableLengthCsvProvider csv = new VariableLengthCsvProvider(new boolean[]
        {
                true, true
        });
        CSVFormat format = CSVFormat.Predefined.Default.getFormat();
        final byte[][] result = csv.decodeCsv(csvData, csvData.length, format);

        Assert.assertEquals("extract field 0", " \tfield1\t , ", new String(result[0]));
        Assert.assertEquals("extract field 1", "field2", new String(result[1]));
    }
}
