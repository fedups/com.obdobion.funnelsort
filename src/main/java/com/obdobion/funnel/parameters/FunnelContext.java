package com.obdobion.funnel.parameters;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.algebrain.Equ;
import com.obdobion.argument.CmdLine;
import com.obdobion.argument.ICmdLine;
import com.obdobion.argument.type.ByteCLA;
import com.obdobion.argument.type.CmdLineCLA;
import com.obdobion.argument.type.WildFiles;
import com.obdobion.funnel.AppContext;
import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.aggregation.Aggregate;
import com.obdobion.funnel.aggregation.AggregateCount;
import com.obdobion.funnel.columns.ColumnHelper;
import com.obdobion.funnel.columns.FormatPart;
import com.obdobion.funnel.columns.HeaderHelper;
import com.obdobion.funnel.columns.HeaderOutHelper;
import com.obdobion.funnel.columns.OutputFormatHelper;
import com.obdobion.funnel.orderby.Filler;
import com.obdobion.funnel.orderby.KeyHelper;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.provider.AbstractInputCache;
import com.obdobion.funnel.provider.ProviderFactory;
import com.obdobion.funnel.publisher.PublisherFactory;

/**
 * <p>
 * FunnelContext class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FunnelContext
{
    static final private Logger logger = LoggerFactory.getLogger(FunnelContext.class);

    static private void showSystemParameters()
    {
        @SuppressWarnings("unchecked")
        final Enumeration<String> pEnumerator = (Enumeration<String>) System.getProperties().propertyNames();
        while (pEnumerator.hasMoreElements())
        {
            final String name = pEnumerator.nextElement();
            // if ("java.library.path".equalsIgnoreCase(name)
            // || "java.endorsed.dirs".equalsIgnoreCase(name)
            // || "sun.boot.library.path".equalsIgnoreCase(name)
            // || "java.class.path".equalsIgnoreCase(name)
            // || "java.home".equalsIgnoreCase(name)
            // || "java.ext.dirs".equalsIgnoreCase(name)
            // || "sun.boot.class.path".equalsIgnoreCase(name))
            // continue;

            if ("line.separator".equalsIgnoreCase(name))
            {
                final byte[] ls = System.getProperties().getProperty(name).getBytes();
                if (ls.length == 1)
                    logger.debug("JVM: {}={}", name, ls[0]);
                else
                    logger.debug("JVM: {}={} {}", name, ls[0], ls[1]);
                continue;
            }
            if ("java.version".equalsIgnoreCase(name))
            {
                logger.debug("Java version: {}", System.getProperties().getProperty(name));
                continue;
            }
            logger.trace("JVM: {}={}", name, System.getProperties().getProperty(name));
        }
    }

    final FunnelSortContext    fsc;

    String[]                   args;
    private int                inputFileIndex;
    public String              specDirectory;
    public FunnelDataProvider  provider;
    public FunnelDataPublisher publisher;
    public AbstractInputCache  inputCache;
    public KeyHelper           keyHelper;
    public OutputFormatHelper  formatOutHelper;
    public HeaderOutHelper     headerOutHelper;
    public ColumnHelper        columnHelper;
    public HeaderHelper        headerHelper;
    public long                comparisonCounter;
    private long               duplicateCount;
    private long               writeCount;
    private long               unselectedCount;
    private long               recordCount;
    private List<KeyPart>      keys;

    /**
     * <p>
     * Constructor for FunnelContext.
     * </p>
     *
     * @param cfg a {@link com.obdobion.funnel.AppContext} object.
     * @param _args a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public FunnelContext(final AppContext cfg, final String... _args) throws IOException, ParseException
    {
        logger.info("================ BEGIN ===================");
        logger.debug("Funnel {}", cfg.version);

        final CmdLine parser = new CmdLine(null, "Funnel is a sort / copy / merge utility.\n\nVersion "
                + cfg.version
                + ".  The log4j configuration file is "
                + cfg.log4jConfigFileName
                + ".");

        if (cfg.specPath != null)
        {
            for (int p = 0; p < cfg.specPath.length; p++)
            {
                parser.addDefaultIncludeDirectory(new File(cfg.specPath[p]));
            }
        }

        fsc = new FunnelSortContext(parser, _args);
        if (isUsageRun())
            return;
        if (isVersion())
        {
            logger.info("version {}", cfg.version);
            System.out.println("Funnel " + cfg.version);
            return;
        }

        /*
         * The parser would normally apply defaults but the generator does not
         * provide for java code to be executed for default values.
         */
        if (fsc.workDirectory == null)
            fsc.workDirectory = new File(System.getProperty("java.io.tmpdir"));

        try
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("commandline:");
            for (final String arg : _args)
                sb.append(" ").append(arg);
            logger.info(sb.toString());

            showSystemParameters();
            postParseAnalysis();
            showParameters();
            provider = ProviderFactory.create(this);
            publisher = PublisherFactory.create(this);
            logger.debug("============= INITIALIZED ================");
        } catch (final ParseException pe)
        {
            // logger.fatal(pe.getMessage());
            pe.fillInStackTrace();
            throw pe;
        }
    }

    /**
     * <p>
     * getAggregateByName.
     * </p>
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    public Aggregate getAggregateByName(final String name)
    {
        if (getAggregates() != null)
            for (final Aggregate agg : getAggregates())
                if (agg.name.equalsIgnoreCase(name))
                    return agg;
        return null;
    }

    /**
     * <p>
     * getAggregates.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<Aggregate> getAggregates()
    {
        return fsc.aggregates;
    }

    /**
     * <p>
     * getCopyOrder.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.parameters.CopyOrder} object.
     */
    public CopyOrder getCopyOrder()
    {
        return fsc.copyOrder;
    }

    /**
     * <p>
     * getCsv.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.parameters.CSVDef} object.
     */
    public CSVDef getCsv()
    {
        return fsc.csv;
    }

    /**
     * <p>
     * getDepth.
     * </p>
     *
     * @return a int.
     */
    public int getDepth()
    {
        return fsc.depth;
    }

    /**
     * <p>
     * Getter for the field <code>duplicateCount</code>.
     * </p>
     *
     * @return a long.
     */
    public long getDuplicateCount()
    {
        return duplicateCount;
    }

    /**
     * <p>
     * getDuplicateDisposition.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.parameters.DuplicateDisposition}
     *         object.
     */
    public DuplicateDisposition getDuplicateDisposition()
    {
        return fsc.duplicateDisposition;
    }

    /**
     * <p>
     * getEndOfRecordDelimiterIn.
     * </p>
     *
     * @return an array of byte.
     */
    public byte[] getEndOfRecordDelimiterIn()
    {
        return fsc.endOfRecordDelimiterIn;
    }

    /**
     * <p>
     * getEndOfRecordDelimiterOut.
     * </p>
     *
     * @return an array of byte.
     */
    public byte[] getEndOfRecordDelimiterOut()
    {
        return fsc.endOfRecordDelimiterOut;
    }

    /**
     * <p>
     * getFixedRecordLengthIn.
     * </p>
     *
     * @return a int.
     */
    public int getFixedRecordLengthIn()
    {
        return fsc.fixedRecordLengthIn;
    }

    /**
     * <p>
     * getFixedRecordLengthOut.
     * </p>
     *
     * @return a int.
     */
    public int getFixedRecordLengthOut()
    {
        return fsc.fixedRecordLengthOut;
    }

    /**
     * <p>
     * getFormatOutDefs.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<FormatPart> getFormatOutDefs()
    {
        return fsc.formatOutDefs;
    }

    /**
     * <p>
     * getHeaderInDefs.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<KeyPart> getHeaderInDefs()
    {
        return fsc.headerInDefs;
    }

    /**
     * <p>
     * getHeaderOutDefs.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<FormatPart> getHeaderOutDefs()
    {
        return fsc.headerOutDefs;
    }

    /**
     * <p>
     * getHexDumps.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<HexDump> getHexDumps()
    {
        return fsc.hexDumps;
    }

    /**
     * <p>
     * getInputColumnDefs.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<KeyPart> getInputColumnDefs()
    {
        return fsc.inputColumnDefs;
    }

    /**
     * <p>
     * getInputFile.
     * </p>
     *
     * @param fileNumber a int.
     * @return a {@link java.io.File} object.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public File getInputFile(final int fileNumber) throws ParseException, IOException
    {
        return fsc.inputFiles.files().get(fileNumber);
    }

    /**
     * <p>
     * getInputFiles.
     * </p>
     *
     * @return a {@link com.obdobion.argument.type.WildFiles} object.
     */
    public WildFiles getInputFiles()
    {
        return fsc.inputFiles;
    }

    /**
     * <p>
     * Getter for the field <code>keys</code>.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<KeyPart> getKeys()
    {
        return keys;
    }

    /**
     * <p>
     * getMaximumNumberOfRows.
     * </p>
     *
     * @return a long.
     */
    public long getMaximumNumberOfRows()
    {
        return fsc.maximumNumberOfRows;
    }

    private List<OrderBy> getOrderBys()
    {
        return fsc.orderBys;
    }

    /**
     * <p>
     * getOutputFile.
     * </p>
     *
     * @return a {@link java.io.File} object.
     */
    public File getOutputFile()
    {
        return fsc.outputFile;
    }

    /**
     * <p>
     * Getter for the field <code>recordCount</code>.
     * </p>
     *
     * @return a long.
     */
    public long getRecordCount()
    {
        return recordCount;
    }

    /**
     * <p>
     * getStopEqu.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<Equ> getStopEqu()
    {
        return fsc.stopEqu;
    }

    /**
     * <p>
     * Getter for the field <code>unselectedCount</code>.
     * </p>
     *
     * @return a long.
     */
    public long getUnselectedCount()
    {
        return unselectedCount;
    }

    /**
     * <p>
     * getWhereEqu.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<Equ> getWhereEqu()
    {
        return fsc.whereEqu;
    }

    /**
     * <p>
     * getWorkDirectory.
     * </p>
     *
     * @return a {@link java.io.File} object.
     */
    public File getWorkDirectory()
    {
        return fsc.workDirectory;
    }

    /**
     * <p>
     * Getter for the field <code>writeCount</code>.
     * </p>
     *
     * @return a long.
     */
    public long getWriteCount()
    {
        return writeCount;
    }

    /**
     * <p>
     * inputCounters.
     * </p>
     *
     * @param p_unselectedCount a long.
     * @param p_recordCount a long.
     */
    public void inputCounters(final long p_unselectedCount, final long p_recordCount)
    {
        unselectedCount += p_unselectedCount;
        recordCount += p_recordCount;

    }

    /**
     * <p>
     * inputFileCount.
     * </p>
     *
     * @return a int.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public int inputFileCount() throws ParseException, IOException
    {
        if (getInputFiles() == null)
            return 0;
        return getInputFiles().files().size();
    }

    /**
     * <p>
     * inputFileIndex.
     * </p>
     *
     * @return a int.
     */
    public int inputFileIndex()
    {
        return inputFileIndex;
    }

    /**
     * <p>
     * isAggregating.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAggregating()
    {
        return getAggregates() != null && !getAggregates().isEmpty();
    }

    /**
     * <p>
     * isCacheInput.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isCacheInput()
    {
        return !fsc.noCacheInput;
    }

    /**
     * <p>
     * isCacheWork.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isCacheWork()
    {
        return !fsc.diskWork;
    }

    /**
     * <p>
     * isDiskWork.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isDiskWork()
    {
        return fsc.diskWork;
    }

    /**
     * <p>
     * isHexDumping.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isHexDumping()
    {
        return fsc.hexDumps != null;
    }

    /**
     * <p>
     * isInPlaceSort.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isInPlaceSort()
    {
        return fsc.inPlaceSort;
    }

    /**
     * <p>
     * isMultisourceInput.
     * </p>
     *
     * @return a boolean.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public boolean isMultisourceInput() throws ParseException, IOException
    {
        return getInputFiles() != null && getInputFiles().files().size() > 1;
    }

    /**
     * <p>
     * isNoCacheInput.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isNoCacheInput()
    {
        return fsc.noCacheInput;
    }

    /**
     * <p>
     * isSyntaxOnly.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isSyntaxOnly()
    {
        return fsc.syntaxOnly;
    }

    /**
     * <p>
     * isSysin.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isSysin()
    {
        return !(fsc.getParser().arg("--inputfilename").isParsed());
    }

    /**
     * <p>
     * isSysout.
     * </p>
     *
     * @return a boolean.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public boolean isSysout() throws ParseException, IOException
    {
        if (isMultisourceInput() && isInPlaceSort())
            return false;
        return getOutputFile() == null;
    }

    /**
     * <p>
     * isUsageRun.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isUsageRun()
    {
        return ((CmdLine) fsc.getParser()).isUsageRun();
    }

    /**
     * <p>
     * isUserSpecifiedOrder.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isUserSpecifiedOrder()
    {
        return getOrderBys() == null || getOrderBys().isEmpty();
    }

    /**
     * <p>
     * isVariableLengthInput.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isVariableLengthInput()
    {
        return fsc.getParser().arg("--variableIn").isParsed() || !(fsc.getParser().arg("--fixedIn").isParsed());
    }

    /**
     * <p>
     * isVariableLengthOutput.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isVariableLengthOutput()
    {
        return fsc.getParser().arg("--variableOutput").isParsed();
    }

    /**
     * <p>
     * isVersion.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isVersion()
    {
        return fsc.version;
    }

    /**
     * <p>
     * outputCounters.
     * </p>
     *
     * @param p_duplicateCount a long.
     * @param p_writeCount a long.
     */
    public void outputCounters(final long p_duplicateCount, final long p_writeCount)
    {
        duplicateCount += p_duplicateCount;
        writeCount += p_writeCount;
    }

    private void postParseAggregation() throws ParseException
    {
        if (getAggregates() != null)
        {
            final List<String> aggregateNamesFoundSoFar = new ArrayList<>();

            for (final Aggregate agg : getAggregates())
            {
                if (aggregateNamesFoundSoFar.contains(agg.name))
                    throw new ParseException("aggregate \"" + agg.name + "\" must have a unique name", 0);
                aggregateNamesFoundSoFar.add(agg.name);

                if (agg instanceof AggregateCount)
                    continue;

                if (columnHelper.exists(agg.name))
                    throw new ParseException("aggregate \"" + agg.name + "\" is already defined as a column", 0);

                if (agg.columnName != null)
                {
                    if (!columnHelper.exists(agg.columnName))
                        throw new ParseException("aggregate \""
                                + agg.name
                                + "\" must reference a defined column: "
                                + agg.columnName, 0);

                    final KeyPart col = columnHelper.get(agg.columnName);
                    if ((col.isNumeric() && !agg.supportsNumber())
                            || (col.isDate() && !agg.supportsDate())
                            || (!col.isNumeric() && !col.isDate()))
                        throw new ParseException("aggregate \""
                                + agg.name
                                + "\" must reference a numeric or date column: "
                                + agg.columnName
                                + " ("
                                + col.typeName
                                + ")", 0);

                    if (agg.equation != null)
                        throw new ParseException("aggregate \""
                                + agg.name
                                + "\" columnName and --equ are mutually exclusive", 0);
                }
            }
        }
    }

    private void postParseAnalysis() throws ParseException, IOException
    {
        columnHelper = new ColumnHelper();
        keyHelper = new KeyHelper();
        formatOutHelper = new OutputFormatHelper(columnHelper, headerHelper);
        headerHelper = new HeaderHelper();
        headerOutHelper = new HeaderOutHelper(headerHelper);

        postParseInputFile();
        postParseHeaderIn();
        postParseHeaderOut();
        postParseInputColumns();
        postParseOrderBy();
        postParseHexDumps();
        postParseAggregation();
        postParseFormatOut();
        postParseOutputFile();
        postParseEolOut();
        postParseCSV();
        postParseFixed();
    }

    private void postParseCSV()
    {
        /*
         * Create a CSV parser if needed.
         */
        if (fsc.getParser().arg("--csv").isParsed())
        {
            getCsv().format = getCsv().predefinedFormat.getFormat();
            logger.debug("defining the CSV parser based on \"{}\"", getCsv().predefinedFormat.name());
            final ICmdLine csvParser = ((CmdLineCLA) fsc.getParser().arg("--csv")).templateCmdLine;

            if (csvParser.arg("--commentMarker").isParsed())
                getCsv().format = getCsv().format.withCommentMarker((char) getCsv().commentMarker);
            if (csvParser.arg("--delimiter").isParsed())
                getCsv().format = getCsv().format.withDelimiter((char) getCsv().delimiter);
            if (csvParser.arg("--escape").isParsed())
                getCsv().format = getCsv().format.withEscape((char) getCsv().escape);
            if (csvParser.arg("--ignoreEmptyLines").isParsed())
                getCsv().format = getCsv().format.withIgnoreEmptyLines(getCsv().ignoreEmptyLines);
            if (csvParser.arg("--ignoreSurroundingSpaces").isParsed())
                getCsv().format = getCsv().format.withIgnoreSurroundingSpaces(getCsv().ignoreSurroundingSpaces);
            if (csvParser.arg("--nullString").isParsed())
                getCsv().format = getCsv().format.withNullString(getCsv().nullString);
            if (csvParser.arg("--quote").isParsed())
                getCsv().format = getCsv().format.withQuote((char) getCsv().quote);
        }
    }

    private void postParseEolOut()
    {
        if (getEndOfRecordDelimiterOut() == null)
            fsc.endOfRecordDelimiterOut = getEndOfRecordDelimiterIn();
    }

    private void postParseFixed() throws ParseException
    {
        if (getFixedRecordLengthOut() > 0 && isVariableLengthOutput())
            throw new ParseException("--fixedOut and --variableOutput are mutually exclusive parameters", 0);
        if (isVariableLengthOutput())
            return;
        if (getFixedRecordLengthOut() == 0)
            fsc.fixedRecordLengthOut = getFixedRecordLengthIn();

    }

    private void postParseFormatOut() throws ParseException
    {
        if (getFormatOutDefs() != null)
        {
            if (getCsv() != null) { throw new ParseException("--csv and --format are mutually exclusive parameters",
                    0); }

            for (final FormatPart kdef : getFormatOutDefs())
            {
                try
                {
                    if (kdef.offset == -1) // unspecified
                        kdef.offset = 0;

                    if (kdef.columnName != null)
                        if (!columnHelper.exists(kdef.columnName))
                        {
                            if (!headerHelper.exists(kdef.columnName))
                            {
                                if (getAggregateByName(kdef.columnName) == null)
                                    throw new ParseException("--formatOut must be a defined column or header: "
                                            + kdef.columnName, 0);
                                throw new ParseException(
                                        "--formatOut must be a defined column, aggregates can only be used within --equ: "
                                                + kdef.columnName,
                                        0);
                            }
                        }
                    if (kdef.columnName != null && kdef.equation != null)
                        throw new ParseException("--formatOut columnName and --equ are mutually exclusive", 0);
                    if (kdef.format != null && kdef.equation == null)
                        throw new ParseException("--formatOut --format is only valid with --equ", 0);

                    if (kdef.equation != null)
                    {
                        if (kdef.length == 255)
                            throw new ParseException("--formatOut --length is required when --equ is specified", 0);
                    }

                    formatOutHelper.add(kdef);
                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }
        }
    }

    private void postParseHeaderIn() throws ParseException
    {
        headerHelper.setWaitingForInput(false);
        if (getHeaderInDefs() != null)
        {
            headerHelper.setWaitingForInput(true);

            // headerInDefs.size() > 1
            // || (headerInDefs.get(0).columnName != null ||
            // headerInDefs.get(0).equation != null)

            /*
             * This may be overridden in the postParseHeaderOut method.
             */
            headerOutHelper.setWaitingToWrite(true);

            KeyPart previousColDef = null;
            for (final KeyPart colDef : getHeaderInDefs())
            {
                try
                {
                    /*
                     * Provide a default length when the format is specified and
                     * the length is not.
                     */
                    if (colDef.length == KeyHelper.MAX_KEY_SIZE
                            && colDef.parseFormat != null
                            && colDef.parseFormat.length() > 0)
                    {
                        colDef.length = colDef.parseFormat.length();
                        logger.debug("column \"{}\" length set to {} because of format", colDef.columnName,
                                colDef.length);
                    }
                    if (getCsv() != null)
                        throw new ParseException("headerIn not supported for csv files", 0);

                    if (colDef.offset == -1) // unspecified
                    {
                        if (previousColDef != null)
                            colDef.offset = previousColDef.offset + previousColDef.length;
                        else
                            colDef.offset = 0;
                    }

                    if (!(colDef instanceof Filler))
                    {
                        if (headerHelper.exists(colDef.columnName))
                            throw new ParseException("headerIn must be unique: " + colDef.columnName, 0);
                        headerHelper.add(colDef);
                    }
                    previousColDef = colDef;

                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }
        }
    }

    private void postParseHeaderOut() throws ParseException
    {
        if (getHeaderOutDefs() != null)
        {
            if (getCsv() != null) { throw new ParseException("--csv and --headerOut are mutually exclusive parameters",
                    0); }
            /*
             * --headerOut(), no args, means to suppress the headerIn from being
             * written.
             */
            headerOutHelper.setWaitingToWrite(getHeaderOutDefs().size() > 1
                    || (getHeaderOutDefs().get(0).columnName != null || getHeaderOutDefs().get(0).equation != null));

            for (final FormatPart kdef : getHeaderOutDefs())
            {
                try
                {
                    if (kdef.offset == -1) // unspecified
                        kdef.offset = 0;

                    if (kdef.columnName != null)
                        if (!headerHelper.exists(kdef.columnName)) { throw new ParseException(
                                "--headerOut must be a defined headerIn: " + kdef.columnName, 0); }
                    if (kdef.columnName != null && kdef.equation != null)
                        throw new ParseException("--headerOut columnName and --equ are mutually exclusive", 0);
                    if (kdef.format != null && kdef.equation == null)
                        throw new ParseException("--headerOut --format is only valid with --equ", 0);

                    if (kdef.equation != null)
                    {
                        if (kdef.length == 255)
                            throw new ParseException("--headerOut --length is required when --equ is specified", 0);
                    }

                    headerOutHelper.add(kdef);
                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }
        }
    }

    private void postParseHexDumps() throws ParseException
    {
        /*
         * Convert OrderBys into sort keys
         */
        if (getHexDumps() != null && !getHexDumps().isEmpty())
        {
            if (getHexDumps().size() == 1 && getHexDumps().get(0).columnName == null)
                /*
                 * Full record dump
                 */
                return;

            if (getAggregates() != null)
                throw new ParseException("HexDump with aggregate processing is not supported", 0);
            if (!isVariableLengthOutput() && (getFixedRecordLengthIn() > 0 || getFixedRecordLengthOut() > 0))
                throw new ParseException("HexDump is only valid with variableOutput", 0);
            if (isInPlaceSort())
                throw new ParseException("HexDump is not valid with --replace", 0);

            for (final HexDump hexDump : getHexDumps())
            {
                if (!columnHelper.exists(hexDump.columnName))
                    throw new ParseException("HexDump must be a defined column: " + hexDump.columnName, 0);
                final KeyPart column = columnHelper.get(hexDump.columnName);
                if (KeyType.String != column.typeName && KeyType.Byte != column.typeName)
                    throw new ParseException("HexDump can only be on String or Byte columns: " + hexDump.columnName, 0);
            }
        }
    }

    private void postParseInputColumns() throws ParseException
    {
        if (getInputColumnDefs() != null)
        {
            KeyPart previousColDef = null;
            for (final KeyPart colDef : getInputColumnDefs())
            {
                try
                {
                    /*
                     * Provide a default length when the format is specified and
                     * the length is not.
                     */
                    if (colDef.length == KeyHelper.MAX_KEY_SIZE
                            && colDef.parseFormat != null
                            && colDef.parseFormat.length() > 0)
                    {
                        colDef.length = colDef.parseFormat.length();
                        logger.debug("column \"{}\" length set to {} because of format", colDef.columnName,
                                colDef.length);
                    }
                    /*
                     * Compute an offset if one was not specified. But only for
                     * non-csv files since offset is not part of the csv
                     * specification.
                     */
                    if (getCsv() == null)
                        if (colDef.offset == -1) // unspecified
                        {
                            if (previousColDef != null)
                                colDef.offset = previousColDef.offset + previousColDef.length;
                            else
                                colDef.offset = 0;
                        }
                    /*
                     * Since the parameter is 1-relative, an arbitrary decision,
                     * we have to subtract one from them before they can be
                     * used.
                     */
                    if (colDef.csvFieldNumber > 0)
                    {
                        colDef.csvFieldNumber--;
                        colDef.offset = 0;
                    }

                    if (!(colDef instanceof Filler))
                    {
                        if (headerHelper.exists(colDef.columnName))
                            throw new ParseException("columnsIn must be unique from headerIn: " + colDef.columnName, 0);
                        columnHelper.add(colDef);
                    }
                    previousColDef = colDef;

                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }
        }
    }

    private void postParseInputFile() throws ParseException, IOException
    {
        if (!isSysin()
                && (getInputFiles() == null || getInputFiles().files() == null || getInputFiles().files().size() == 0))
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("file not found");
            if (getInputFiles() != null)
            {
                sb.append(": ");
                sb.append(getInputFiles().toString());
            }
            throw new ParseException(sb.toString(), 0);
        }
    }

    private void postParseOrderBy() throws ParseException
    {
        /*
         * Convert OrderBys into sort keys
         */
        if (getOrderBys() != null && !getOrderBys().isEmpty())
        {
            if (keys == null)
                keys = new ArrayList<>();
            for (final OrderBy orderBy : getOrderBys())
            {
                if (!columnHelper.exists(orderBy.columnName))
                    throw new ParseException("OrderBy must be a defined column: " + orderBy.columnName, 0);
                final KeyPart col = columnHelper.get(orderBy.columnName);
                final KeyPart newKey = col.newCopy();
                newKey.direction = orderBy.direction;
                keys.add(newKey);
            }
        }

        if (keys == null)
            keyHelper.setUpAsCopy(this);
        /*
         * Check for cvs keys on a non-cvs file
         */
        if (keys != null && getCsv() == null)
            for (final KeyPart kdef : keys)
            {
                if (kdef.isCsv()) { throw new ParseException("unexpected CSV key (--field) on a non-CSV file", 0); }
            }
        /*
         * Check for non-cvs keys on a cvs file
         */
        if (keys != null && getCsv() != null)
            for (final KeyPart kdef : keys)
            {
                if (!kdef.isCsv()) { throw new ParseException("only CSV keys (--field) allowed on a CSV file", 0); }
            }
        /*
         * Check for duplicate csv keys
         */
        if (keys != null && getCsv() != null)
            for (final KeyPart k1 : keys)
            {
                for (final KeyPart k2 : keys)
                {
                    if (k1 != k2 && k1.csvFieldNumber == k2.csvFieldNumber) { throw new ParseException(
                            "sorting on the same field (--field "
                                    + k2.csvFieldNumber
                                    + ") is not allowed",
                            0); }
                }
            }

        if (keys != null)
            for (final KeyPart kdef : keys)
            {
                try
                {
                    keyHelper.add(kdef, columnHelper);
                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }
    }

    private void postParseOutputFile() throws ParseException, IOException
    {
        if (isInPlaceSort() && getOutputFile() != null)
            throw new ParseException("--replace and --outputFile are mutually exclusive parameters", 0);

        if (isInPlaceSort() && isSysin())
            throw new ParseException("--replace requires --inputFile, redirection or piped input is not allowed", 0);

        /*
         * -r is how the input file is replaced. If we set the outputFile here
         * it then becomes impossible to sort to the command line (sysout).
         */
        if (isInPlaceSort())
            fsc.outputFile = getInputFile(0);
    }

    /**
     * <p>
     * reset.
     * </p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void reset() throws IOException, ParseException
    {
        if (provider != null)
            provider.reset();
        if (publisher != null)
            publisher.reset();
    }

    /**
     * <p>
     * setDepth.
     * </p>
     *
     * @param optimalFunnelDepth a int.
     */
    public void setDepth(final int optimalFunnelDepth)
    {
        fsc.depth = optimalFunnelDepth;
    }

    /**
     * <p>
     * setInputFiles.
     * </p>
     *
     * @param wildFiles a {@link com.obdobion.argument.type.WildFiles} object.
     */
    public void setInputFiles(final WildFiles wildFiles)
    {
        fsc.inputFiles = wildFiles;
    }

    /**
     * <p>
     * setOutputFile.
     * </p>
     *
     * @param outputFile a {@link java.io.File} object.
     */
    public void setOutputFile(final File outputFile)
    {
        fsc.outputFile = outputFile;
    }

    /**
     *
     */
    void showParameters() throws IOException, ParseException
    {
        if (isSysin())
            showParametersLog(true, "input is SYSIN");
        else
            for (final File file : getInputFiles().files())
                showParametersLog(true, "inputFilename = {}", file.getAbsolutePath());

        if (isCacheInput())
            showParametersLog(false, "input caching enabled");

        if (isSysout())
            showParametersLog(true, "output is SYSOUT");
        else if (isInPlaceSort())
            showParametersLog(true, "outputFilename= input file name");
        else
            showParametersLog(true, "outputFilename= {}", getOutputFile().getAbsolutePath());

        if (isCacheWork())
            showParametersLog(false, "work files are cached in memory");
        else if (getWorkDirectory() != null)
            showParametersLog(false, "work directory= {}", getWorkDirectory().getAbsolutePath());

        if (specDirectory != null)
            showParametersLog(false, "specification include path is {}", specDirectory);

        if (getFixedRecordLengthIn() > 0)
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("FixedIn  = ").append(getFixedRecordLengthIn());
            if (isVariableLengthOutput())
                sb.append(" adding VLR delimiters on output");
            showParametersLog(false, sb.toString());
        } else
        {
            if (getMaximumNumberOfRows() != Long.MAX_VALUE)
                showParametersLog(false, "max rows= {}", getMaximumNumberOfRows());

            final StringBuilder bytes = new StringBuilder();
            bytes.append("in:");
            for (int b = 0; b < getEndOfRecordDelimiterIn().length; b++)
            {
                bytes.append(" ");
                bytes.append(ByteCLA.asLiteral(getEndOfRecordDelimiterIn()[b]));
            }
            if (getFixedRecordLengthOut() == 0)
            {
                bytes.append(", out:");
                for (int b = 0; b < getEndOfRecordDelimiterOut().length; b++)
                {
                    bytes.append(" ");
                    bytes.append(ByteCLA.asLiteral(getEndOfRecordDelimiterOut()[b]));
                }
            }

            showParametersLog(false, "End of line delimeter {}", bytes.toString());

            if (getCsv() != null)
            {
                final StringBuilder csvMsg = new StringBuilder();
                csvMsg.append("csv: ");
                if (getCsv().header)
                    csvMsg.append("has header");
                else
                    csvMsg.append("no header");
                showParametersLog(false, csvMsg.toString());
            }
        }
        if (getFixedRecordLengthOut() > 0)
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("FixedOut = ").append(getFixedRecordLengthOut());
            showParametersLog(false, sb.toString());
        }

        showParametersLog(false, "power   = {}", getDepth());

        if (getDuplicateDisposition() != DuplicateDisposition.Original)
            showParametersLog(false, "dups    = {}", getDuplicateDisposition().name());

        for (final String colName : columnHelper.getNames())
        {
            final KeyPart col = columnHelper.get(colName);
            if (getCsv() == null)
                showParametersLog(false, "col \"{}\" {} offset {} length {} {}", col.columnName, col.typeName,
                        col.offset, col.length, (col.parseFormat == null
                                ? ""
                                : " format " + col.parseFormat));
            else
                showParametersLog(false, "col {} {} csvField {} {}", col.columnName, col.typeName, col.csvFieldNumber,
                        (col.parseFormat == null
                                ? ""
                                : " format " + col.parseFormat));
        }

        for (final String colName : headerHelper.getNames())
        {
            final KeyPart col = headerHelper.get(colName);
            showParametersLog(false, "headerIn \"{}\" {} offset {} length {} {}", col.columnName, col.typeName,
                    col.offset, col.length, (col.parseFormat == null
                            ? ""
                            : " format " + col.parseFormat));
        }

        if (getAggregates() != null)
            for (final Aggregate agg : getAggregates())
            {
                if (agg instanceof AggregateCount)
                    showParametersLog(false, "aggregate \"count\"");
                else
                    showParametersLog(false, "aggregate \"{}\" {}", agg.name, (agg.columnName == null
                            ? agg.equation.toString()
                            : agg.columnName));
            }

        if (getWhereEqu() != null)
        {
            for (final Equ equ : getWhereEqu())
            {
                showParametersLog(true, "where \"{}\"", equ.toString());
            }
        }

        if (getStopEqu() != null)
        {
            for (final Equ equ : getStopEqu())
            {
                showParametersLog(true, "stopWhen \"{}\"", equ.toString());
            }
        }

        if (keys == null)
            logger.debug("process = {} order", getCopyOrder().name());
        else
            for (final KeyPart def : keys)
            {
                showParametersLog(false, "orderBy {} {}", def.columnName, def.direction.name());
            }

        if (getFormatOutDefs() != null)
            for (final FormatPart outDef : getFormatOutDefs())
            {
                final StringBuilder sb = new StringBuilder();
                sb.append("format ");
                if (outDef.columnName != null)
                    sb.append("\"").append(outDef.columnName).append("\"");
                if (outDef.equation != null)
                    sb.append("\"").append(outDef.equation.toString()).append("\"");
                if (outDef.typeName != null)
                    sb.append(" ").append(outDef.typeName.name());
                if (outDef.format != null)
                    sb.append(" format \"").append(outDef.format).append("\"");
                if (outDef.filler != 0x00)
                    sb.append(" fill=").append(ByteCLA.asLiteral(outDef.filler));
                if (outDef.length != 255)
                    sb.append(" length ").append(outDef.length);
                if (outDef.offset != 0)
                    sb.append(" offset ").append(outDef.offset);
                if (outDef.size != 255)
                    sb.append(" size ").append(outDef.size);

                showParametersLog(false, sb.toString());

                if (outDef.equation != null)
                {
                    try
                    {
                        logger.trace("\n{}", outDef.equation.showRPN());
                    } catch (final Exception e)
                    {
                        logger.warn("algebrain", e);
                    }
                }
            }

        if (getHeaderOutDefs() != null)
            for (final FormatPart outDef : getHeaderOutDefs())
            {
                final StringBuilder sb = new StringBuilder();
                sb.append("headerOut ");
                if (outDef.columnName != null)
                    sb.append("\"").append(outDef.columnName).append("\"");
                if (outDef.equation != null)
                    sb.append("\"").append(outDef.equation.toString()).append("\"");
                if (outDef.typeName != null)
                    sb.append(" ").append(outDef.typeName.name());
                if (outDef.format != null)
                    sb.append(" format \"").append(outDef.format).append("\"");
                if (outDef.filler != 0x00)
                    sb.append(" fill=").append(ByteCLA.asLiteral(outDef.filler));
                if (outDef.length != 255)
                    sb.append(" length ").append(outDef.length);
                if (outDef.offset != 0)
                    sb.append(" offset ").append(outDef.offset);
                if (outDef.size != 255)
                    sb.append(" size ").append(outDef.size);

                showParametersLog(false, sb.toString());

                if (outDef.equation != null)
                {
                    try
                    {
                        logger.trace("\n{}", outDef.equation.showRPN());
                    } catch (final Exception e)
                    {
                        logger.warn("algebrain", e);
                    }
                }
            }
    }

    private void showParametersLog(final boolean forceInfo, final String message, final Object... parms)
    {
        if (forceInfo || isSyntaxOnly())
            logger.info(message, parms);
        else
            logger.debug(message, parms);
    }

    /**
     * <p>
     * startNextInput.
     * </p>
     *
     * @return a boolean.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public boolean startNextInput() throws ParseException, IOException
    {
        /*
         * Has the last input file been read? Then return false.
         */
        if (inputFileIndex() >= (inputFileCount() - 1))
            return false;
        inputFileIndex++;
        return true;
    }

    /**
     * <p>
     * stopIsTrue.
     * </p>
     *
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public boolean stopIsTrue() throws Exception
    {
        if (getStopEqu() == null)
            return false;

        for (final Equ equ : getStopEqu())
        {
            /*
             * All of the stop equations must be true.
             */
            final Object result = equ.evaluate();
            if (result == null)
                return false;
            if (!(result instanceof Boolean))
                throw new Exception("--stopWhen clause must evaluate to true or false");
            if (!((Boolean) result).booleanValue())
                return false;
        }
        return true;
    }

    /**
     * <p>
     * whereIsTrue.
     * </p>
     *
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public boolean whereIsTrue() throws Exception
    {
        if (getWhereEqu() == null)
            return true;

        for (final Equ equ : getWhereEqu())
        {
            /*
             * All of the where equations must be true.
             */
            final Object result = equ.evaluate();
            if (result == null)
                return false;
            if (!(result instanceof Boolean))
                throw new Exception("--where clause must evaluate to true or false");
            if (!((Boolean) result).booleanValue())
                return false;
        }
        return true;
    }

}
