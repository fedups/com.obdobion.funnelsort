package com.obdobion.funnel;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * ExampleTest class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class ExampleTest
{
    /**
     * <p>
     * addHeader.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void addHeader() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * aggregates.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void aggregates() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 6L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * averageRowsPerSecond.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void averageRowsPerSecond() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 1L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * convertFixedToVariable.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void convertFixedToVariable() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * convertVariableToFixed.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void convertVariableToFixed() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * copyCollate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void copyCollate() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 104L, context.getRecordCount());
        Assert.assertEquals("records out", 104L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * copyOriginal.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void copyOriginal() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * copyReverse.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void copyReverse() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * countJobs.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void countJobs() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 1L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * createFile.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void createFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = new File("c:/tmp/MyDataVariable.out");

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
        Assert.assertTrue("delete", output.delete());
    }

    /**
     * <p>
     * csvSort.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void csvSort() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 53L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * dos2unix.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void dos2unix() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * dupFirstOnly.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void dupFirstOnly() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 2L, context.getWriteCount());
        Assert.assertEquals("records dup", 21L, context.getDuplicateCount());
    }

    /**
     * <p>
     * eolWord.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void eolWord() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * formatComputedColumn.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void formatComputedColumn() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * formatDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void formatDate() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * formatFiller.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void formatFiller() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * formatFormatNumber.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void formatFormatNumber() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * formatTwoColumns.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void formatTwoColumns() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * hexDumpColumns.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void hexDumpColumns() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 7L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * hexDumpRow.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void hexDumpRow() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 7L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * multiKey.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void multiKey() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * oneKey.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void oneKey() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 28L, context.getRecordCount());
        Assert.assertEquals("records out", 28L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * orderByAbsInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void orderByAbsInt() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * orderByDescDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void orderByDescDate() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 8L, context.getDuplicateCount());
    }

    /**
     * <p>
     * orderByFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void orderByFloat() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 5L, context.getDuplicateCount());
    }

    /**
     * <p>
     * orderByInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void orderByInt() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * orderWithHeader.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void orderWithHeader() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * removeHeader.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void removeHeader() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * sortMultipleFiles.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sortMultipleFiles() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 58L, context.getRecordCount());
        Assert.assertEquals("records out", 58L, context.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.getDuplicateCount());
    }

    /**
     * <p>
     * sortSingleFile.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sortSingleFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * sortWildFile.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sortWildFile() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 58L, context.getRecordCount());
        Assert.assertEquals("records out", 58L, context.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.getDuplicateCount());
    }

    /**
     * <p>
     * stopAtRecordNumber.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void stopAtRecordNumber() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 10L, context.getRecordCount());
        Assert.assertEquals("records out", 10L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * stopAtTimestamp.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void stopAtTimestamp() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 19L, context.getRecordCount());
        Assert.assertEquals("records out", 19L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * upperCase.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void upperCase() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * wherePattern.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void wherePattern() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 4L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    /**
     * <p>
     * whereRange.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void whereRange() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 8L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

}
