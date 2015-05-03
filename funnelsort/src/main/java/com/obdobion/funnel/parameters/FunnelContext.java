package com.obdobion.funnel.parameters;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.obdobion.algebrain.Equ;
import com.obdobion.argument.ByteCLA;
import com.obdobion.argument.CmdLine;
import com.obdobion.argument.input.CommandLineParser;
import com.obdobion.argument.input.IParserInput;
import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.columns.ColumnHelper;
import com.obdobion.funnel.orderby.KeyDirection;
import com.obdobion.funnel.orderby.KeyHelper;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.provider.InputCache;
import com.obdobion.funnel.provider.ProviderFactory;
import com.obdobion.funnel.publisher.PublisherFactory;

/**
 * @author Chris DeGreef
 *
 */
public class FunnelContext
{
    private static final String MASTER_LOG_NAME   = "MasterAppender";
    private static final String SYSPARM_SPEC_PATH = "specPath";
    public static final String  SYSPARM_DEBUG     = "debug";
    public static final String  SYSPARM_VERSION   = "version";
    private static final String ENVVAR_USERHOME   = "user.home";

    static final private Logger logger            = Logger.getLogger(FunnelContext.class);

    static private void defineCacheInput (
            final List<String> def)
    {
        def.add("-tBoolean -k cacheInput --var cacheInput --def false -h 'Read the input file into memory.  This saves reading it again on multipass sorts.  The amount of memory required to hold the input file in core is equal to the size of the file.'");
    }

    static private void defineCacheWork (
            final List<String> def)
    {
        def.add("-tBoolean -k cacheWork --var cacheWork --def false -h 'Work files are saved in memory.  Otherwise they are stored on disk.  The amount of memory required to hold work areas in memory is about (2 * (keySize + 24)).'");
    }

    static private void defineColumnsInSubParser (
            final ArrayList<String> def)
    {
        def.add("-tBegin -k columnsIn -m1 --var inputColumnDefs --factoryMethod "
                + KeyType.class.getName()
                + ".create --factoryA '--type' -h 'Column definitions defining the input file layout.'");
        defineKeyName(def);
        defineKeyType(def);
        defineKeyCSVField(def);
        defineKeyOffset(def);
        defineKeyLength(def);
        defineKeyFormat(def);
        def.add("-tEnd -k columnsIn");
    }

    static private void defineCopyOrder (
            final ArrayList<String> def)
    {
        def.add("-tEnum -k c copy --var copyOrder --def "
                + CopyOrder.ByKey.name()
                + " -h 'Defines the process that will take place on the input.' --case --enumlist "
                + CopyOrder.class.getName());
    }

    static private void defineCsvHeader (
            final ArrayList<String> def)
    {
        def.add("-tBoolean -k h header --var header -h'Skip over the first line for sorting and just write it to the beginning of the output file.'");
    }

    static private void defineCSVInSubParser (
            final ArrayList<String> def)
    {
        def.add("-tBegin -k csv --var csv -h 'The definition of the CSV file being read as input.  Using this indicates that the input is in fact a CSV file and the keys paramater should use the --field arguments.'");
        defineCsvHeader(def);
        defineCsvQuoteByte(def);
        defineCsvSeparatorByte(def);
        def.add("-tEnd -k csv");
    }

    static private void defineCsvQuoteByte (
            final ArrayList<String> def)
    {
        def.add("-tByte -k q quoteByte --var quoteByte -h'This is the single character that you would like to use for a quote.' --def '\"'");
    }

    static private void defineCsvSeparatorByte (
            final ArrayList<String> def)
    {
        def.add("-tByte -k s separatorByte --var separatorByte -h'This is the single character that you would like to use for the field separator.' --def ','");
    }

    static private void defineDuplicateHandling (
            final ArrayList<String> def)
    {
        def.add("-tEnum -k d duplicate --var duplicateDisposition --def "
                + DuplicateDisposition.Original.name()
                + " -h 'Special handling of duplicate keyed rows.' --case --enumlist "
                + DuplicateDisposition.class.getName());
    }

    static private void defineEol (
            final ArrayList<String> def)
    {
        final StringBuilder bytes = new StringBuilder();
        for (int b = 0; b < System.getProperty("line.separator").getBytes().length; b++)
        {
            bytes.append(" ");
            bytes.append(ByteCLA.ByteLiteral[System.getProperty("line.separator").getBytes()[b]]);
        }

        def.add("-tByte -keol --var endOfRecordDelimiter -m1 -h 'The byte(s) that end each line in a variable length record file.' --def "
                + bytes.toString());
    }

    static private void defineEolOut (
            final ArrayList<String> def)
    {
        def.add("-tByte -keolOut --var endOfRecordOutDelimiter -m1 -h 'The byte(s) that end each line in a variable length record file.  This will be used to write the output file.  If this is not specified then the --eol value will be used.'");
    }

    static private void defineFixedLength (
            final ArrayList<String> def)
    {
        def.add("-tInteger -k f fixed --var fixedRecordLength -h 'The record length in a fixed record length file.' --ran 1 4096");
    }

    static private void defineInPlaceSort (
            final ArrayList<String> def)
    {
        def.add("-tBoolean -k r replace --var inPlaceSort -h 'Overwrite the input file with the results.  --outputFile is not required with this parameter.  --outputFile is assumed.'");
    }

    static private void defineInputFile (
            final ArrayList<String> def)
    {
        def.add("-tWildFile -k inputfilename --var inputFiles -m1 -ph 'The input file or files to be processed.  Wild cards are allowing in the filename only, not the path.  Sysin is assumed if this parameter is not provided.' --case");
    }

    static private void defineKeyCSVField (
            final ArrayList<String> def)
    {
        def.add("-tInteger -k f field --var csvFieldNumber -h'If this is a CSV file then use this instead of offset and length.' --range 0");
    }

    static private void defineKeyDirection (
            final ArrayList<String> def)
    {
        def.add("-tEnum -k d direction --var direction -p --def "
                + KeyDirection.ASC.name()
                + " -h'The direction of the sort for this key. AASC and ADESC are absolute values of the key - the case of letters would not matter and the sign of numbers would not matter.' "
                + " --case --enumList "
                + KeyDirection.class.getName());
    }

    static private void defineKeyFormat (
            final ArrayList<String> def)
    {
        def.add("-tString -k f format --var parseFormat --case -h'The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.'");
    }

    static private void defineKeyLength (
            final ArrayList<String> def)
    {
        def.add("-tInteger -k l length --var length --def "
                + KeyHelper.MAX_KEY_SIZE
                + " -h'The length of the key in bytes.' --range 1 "
                + KeyHelper.MAX_KEY_SIZE);
    }

    /*
     * The name will always be lower case.
     */
    static private void defineKeyName (
            final ArrayList<String> def)
    {
        def.add("-tString -k n name --var columnName -h'A name for this column / key so that it can be referenced.'");
    }

    /*
     * The name will always be lower case.
     */
    static private void defineKeyNamePositional (
            final ArrayList<String> def)
    {
        def.add("-tString -k n name --var columnName --pos --req -h'A column name to be sorted.'");
    }

    static private void defineKeyOffset (
            final ArrayList<String> def)
    {
        def.add("-tInteger -k o offset --var offset --def 0 -h'The zero relative offset from the beginning of a row.' --range 0");
    }

    static private void defineKeySubParser (
            final ArrayList<String> def)
    {
        def.add("-tBegin -k k keys -m1 --var keys --factoryMethod "
                + KeyType.class.getName()
                + ".create --factoryA '--type' -h 'The sort keys.'");
        defineKeyName(def);
        defineKeyType(def);
        defineKeyCSVField(def);
        defineKeyOffset(def);
        defineKeyLength(def);
        defineKeyDirection(def);
        defineKeyFormat(def);
        def.add("-tEnd -k k");
    }

    /**
     * This is an alternative to --keys. This only allows references to
     * previously defined column names.
     *
     * @param def
     */
    static private void defineOrderBySubParser (
            final ArrayList<String> def)
    {
        def.add("-tBegin -k orderby -m1 --var orderBys -h 'The sort keys defined from columns.'");
        defineKeyNamePositional(def);
        defineKeyDirection(def);
        def.add("-tEnd -k orderby");
    }

    static private void defineKeyType (
            final ArrayList<String> def)
    {
        def.add("-tEnum -k t type -p --var typeName --req --case -h'The data type of the key in the file.' --case --enumList "
                + KeyType.class.getName());
    }

    static private void defineMaxRows (
            final ArrayList<String> def)
    {
        def.add("-tLong -kmaxrows --var maximumNumberOfRows --range 2 --def "
                + Long.MAX_VALUE
                + " -h 'Used for variable length input, estimate the number of rows.  Too low could cause problems.'");
    }

    static private void defineOutputFile (
            final ArrayList<String> def)
    {
        def.add("-tFile -ko outputfilename --var outputFile --case -h 'The output file to be written.  Sysout is assumed if this parameter is not provided.  The same name as the input file is allowed.'");
    }

    static private void definePower (
            final ArrayList<String> def)
    {
        def.add("-tInteger -kpower --var depth --def 16 --range 2 16 -h 'The depth of the funnel.  The bigger this number is, the more memory will be used.  This is computed when --max or -f is specified.'");
    }

    static private void defineVariableOutput (
            final ArrayList<String> def)
    {
        def.add("-tBoolean -k variableoutput --var variableLengthOutput -h 'Use this to cause a fixed input to be written as variable.'");
    }

    static private void defineVersion (
            final ArrayList<String> def)
    {
        def.add("-tBoolean -kversion --var version -h 'Display the version of Funnel'");
    }

    static private void defineWhere (
            final ArrayList<String> def)
    {
        def.add("-tString -k w where --var whereClause -h 'Rows that evaluate to TRUE are selected for Output.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.'");
    }

    static private void defineWorkDirectory (
            final ArrayList<String> def)
    {
        def.add("-tFile -k workDirectory --var workDirectory --case --def /tmp -h 'The directory where temp files will be handled.'");
    }

    static private void showSystemParameters ()
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
                    logger.debug("JVM: " + name + "=" + ls[0]);
                else
                    logger.debug("JVM: " + name + "=" + ls[0] + " " + ls[1]);
                continue;
            }
            if ("user.name".equals(name))
            {
                logger.info(name + "=" + System.getProperties().getProperty(name));
                continue;
            }
            if ("user.home".equals(name)
                    || "user.dir".equals(name))
            {
                logger.debug(name + "=" + System.getProperties().getProperty(name));
                continue;
            }
            logger.trace("JVM: " + name + "=" + System.getProperties().getProperty(name));
        }
    }

    final public CmdLine        parser;
    String[]                    args;

    public List<KeyPart>        keys;
    public OrderBy[]            orderBys;

    public List<KeyPart>        inputColumnDefs;

    public String               whereClause;
    private int                 inputFileIndex;
    public File[]               inputFiles;
    public File                 outputFile;
    public int                  fixedRecordLength;
    public long                 maximumNumberOfRows;
    public int                  depth;
    public boolean              variableLengthOutput;
    public CopyOrder            copyOrder;
    public DuplicateDisposition duplicateDisposition;
    public File                 workDirectory;
    public File                 logDirectory;
    public String               specDirectory;
    public boolean              inPlaceSort;
    public boolean              version;
    public FunnelDataProvider   provider;
    public FunnelDataPublisher  publisher;
    public InputCache           inputCache;
    public boolean              cacheInput;
    public boolean              cacheWork;
    public boolean              debug;
    public KeyHelper            keyHelper;
    public ColumnHelper         columnHelper;
    private final String        installedVersionNumber;
    public byte[]               endOfRecordDelimiter;
    public byte[]               endOfRecordOutDelimiter;
    public CSVDef               csv;

    public FunnelContext(
            final String... _args)
            throws IOException,
            ParseException
    {
        this.debug = System.getProperty(SYSPARM_DEBUG, "NO").equalsIgnoreCase("on");

        installedVersionNumber = System.getProperty(SYSPARM_VERSION, "missing -D" + SYSPARM_VERSION + " parameter");

        /*
         * The log directory is specified here so that the log can be used
         * immediately.
         */
        startLogging(logDirectory = new File(System.getProperty(ENVVAR_USERHOME, "/tmp"),
                "funnel/var/log"));
        logger.info("================ BEGIN ===================");
        logger.debug("Funnel " + installedVersionNumber);

        parser = new CmdLine(null,
                "Funnel is a sort / copy / merge utility.\n\nVersion "
                        + installedVersionNumber
                        + ".  The log file is located in "
                        + logDirectory.getAbsolutePath()
                        + ".");
        /*
         * The specification directory is passed to the parser to control where
         * the default include (import) files are located. This is passed in to
         * Funnel via a JVM parameter (-DspecPath=<path>).
         */
        specDirectory = System.getProperty(SYSPARM_SPEC_PATH, null);
        if (specDirectory != null)
        {
            final String[] pathParts = specDirectory.split("[,;]");
            for (int p = 0; p < pathParts.length; p++)
            {
                parser.addDefaultIncludeDirectory(new File(pathParts[p]));
            }
        }

        final ArrayList<String> def = new ArrayList<>();

        defineInputFile(def);
        defineOutputFile(def);
        defineFixedLength(def);
        defineInPlaceSort(def);
        defineColumnsInSubParser(def);
        defineWhere(def);
        defineEol(def);
        defineEolOut(def);
        defineDuplicateHandling(def);
        defineCopyOrder(def);
        defineMaxRows(def);
        defineKeySubParser(def);
        defineOrderBySubParser(def);
        defineCSVInSubParser(def);
        defineVariableOutput(def);
        defineWorkDirectory(def);
        defineCacheInput(def);
        defineCacheWork(def);
        definePower(def);
        defineVersion(def);

        parser.compile(def);

        final IParserInput cmdline = CommandLineParser.getInstance('-', _args);
        parser.parse(cmdline, this);
        if (parser.isUsageRun())
            return;
        if (version)
        {
            logger.info("version " + installedVersionNumber);
            System.out.println("Funnel " + installedVersionNumber);
            return;
        }

        try
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("commandline: ");
            parser.exportCommandLine(sb);
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

    public File getInputFile (
            final int fileNumber)
    {
        return inputFiles[fileNumber];
    }

    public int inputFileCount ()
    {
        if (inputFiles == null)
            return 0;
        return inputFiles.length;
    }

    public int inputFileIndex ()
    {
        return inputFileIndex;
    }

    public boolean isCacheInput ()
    {
        return cacheInput;
    }

    public boolean isCacheWork ()
    {
        return cacheWork;
    }

    public boolean isInPlaceSort ()
    {
        return inPlaceSort;
    }

    public boolean isMultisourceInput ()
    {
        return inputFiles != null && inputFiles.length > 1;
    }

    public boolean isSysin ()
    {
        return inputFiles == null || inputFiles.length == 0;
    }

    public boolean isSysout ()
    {
        if (isMultisourceInput() && isInPlaceSort())
            return false;
        return outputFile == null;
    }

    private void postParseAnalysis ()
            throws ParseException
    {
        /*
         * OrderBys and keys are mutually exclusive.
         */
        if (orderBys != null && keys != null)
            throw new ParseException("--orderBy and --key can not be used in the same sort", 0);

        columnHelper = new ColumnHelper();
        keyHelper = new KeyHelper();
        /*
         * Save the columns for later referencing
         */
        if (inputColumnDefs != null)
            for (final KeyPart colDef : inputColumnDefs)
            {
                try
                {
                    columnHelper.add(colDef);

                } catch (final Exception e)
                {
                    throw new ParseException(e.getMessage(), 0);
                }
            }

        /*
         * Convert OrderBys into sort keys
         */
        if (orderBys != null && orderBys.length > 0)
        {
            if (keys == null)
                keys = new ArrayList<>();
            for (final OrderBy orderBy : orderBys)
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
        if (keys != null && csv == null)
            for (final KeyPart kdef : keys)
            {
                if (kdef.isCsv())
                {
                    throw new ParseException("unexpected CSV key (--field) on a non-CSV file",
                            0);
                }
            }
        /*
         * Check for non-cvs keys on a cvs file
         */
        if (keys != null && csv != null)
            for (final KeyPart kdef : keys)
            {
                if (!kdef.isCsv())
                {
                    throw new ParseException("only CSV keys (--field) allowed on a CSV file",
                            0);
                }
            }
        /*
         * Check for duplicate csv keys
         */
        if (keys != null && csv != null)
            for (final KeyPart k1 : keys)
            {
                for (final KeyPart k2 : keys)
                {
                    if (k1 != k2 && k1.csvFieldNumber == k2.csvFieldNumber)
                    {
                        throw new ParseException("sorting on the same field (--field "
                                + k2.csvFieldNumber
                                + ") is not allowed",
                                0);
                    }
                }
            }
        /*
         * Provide a default length when the format is specified and the length
         * is not.
         */
        int kidx = 0;
        if (keys != null)
            for (final KeyPart kdef : keys)
            {
                kidx++;
                if (kdef.length == KeyHelper.MAX_KEY_SIZE && kdef.parseFormat != null && kdef.parseFormat.length() > 0)
                {
                    kdef.length = kdef.parseFormat.length();
                    logger.debug("key " + kidx + " length set to " + kdef.length + " because of format");
                }
                // if (csv == null)
                // kdef.csvFieldNumber = -1;
            }
        /*
         * Scan the key defs to see if there are any column definitions. That
         * would be a name and the length (at least) information. Since orderBys
         * are references to columns, and orderBys and keys together don't make
         * sense, we don't have to try and add columns based on orderBy generate
         * keys - they are already columns and would cause an exception.
         */
        if (orderBys == null && keys != null)
            for (final KeyPart keyDefPosingAsAColumnDefToo : keys)
            {
                if (keyDefPosingAsAColumnDefToo.columnName != null
                        && (keyDefPosingAsAColumnDefToo.isCsv() || keyDefPosingAsAColumnDefToo.length > 0))
                {
                    try
                    {
                        columnHelper.add(keyDefPosingAsAColumnDefToo);
                    } catch (final Exception e)
                    {
                        throw new ParseException(e.getMessage(), 0);
                    }
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
                    throw new ParseException(e.getMessage(),
                            0);
                }
            }

        if (isInPlaceSort() && outputFile != null)
            throw new ParseException("--replace and --outputFile are mutually exclusive parameters",
                    0);

        if (isInPlaceSort() && isSysin())
            throw new ParseException("--replace requires --inputFile, redirection or piped input is not allowed",
                    0);

        if (outputFile == null
                && (inputFiles != null
                && (inputFiles.length == 1 || isInPlaceSort())))
            outputFile = getInputFile(0);

        if (endOfRecordOutDelimiter == null)
            endOfRecordOutDelimiter = endOfRecordDelimiter;

        if (whereClause != null)
            try
            {
                Equ.getInstance().compile(whereClause);

            } catch (final Exception e)
            {
                throw new ParseException(e.getMessage(), 0);
            }
    }

    public void reset ()
            throws IOException
    {
        if (provider != null)
            provider.reset();
        if (publisher != null)
            publisher.reset();
    }

    /**
     *
     */
    void showParameters ()
    {

        if (isSysin())
            logger.info("input is SYSIN");
        else
            for (final File inputFile : inputFiles)
                logger.info("inputFilename = " + inputFile.getAbsolutePath());

        if (isCacheInput())
            if (logger.isDebugEnabled())
                logger.info("input caching enabled");

        if (isSysout())
            logger.info("output is SYSOUT");
        else if (isInPlaceSort())
            logger.info("outputFilename= input file name");
        else
            logger.info("outputFilename= " + outputFile.getAbsolutePath());

        if (isCacheWork())
            logger.debug("work files are cached in memory");
        else
            logger.debug("work directory= " + workDirectory.getAbsolutePath());

        if (specDirectory != null)
            logger.debug("specification include path is " + specDirectory);
        if (specDirectory != null)
            logger.debug("The log file is in this directory: " + logDirectory.getAbsolutePath());

        if (fixedRecordLength > 0)
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("LRecL   = ").append(fixedRecordLength);
            if (variableLengthOutput)
                sb.append(" adding VLR delimiters on output");
            logger.info(sb.toString());
        } else
        {
            if (maximumNumberOfRows != Long.MAX_VALUE)
                logger.debug("max rows= " + maximumNumberOfRows);

            final StringBuilder bytes = new StringBuilder();
            bytes.append("in:");
            for (int b = 0; b < endOfRecordDelimiter.length; b++)
            {
                bytes.append(" ");
                bytes.append(ByteCLA.asLiteral(endOfRecordDelimiter[b]));
            }
            bytes.append(", out:");
            for (int b = 0; b < endOfRecordOutDelimiter.length; b++)
            {
                bytes.append(" ");
                bytes.append(ByteCLA.asLiteral(endOfRecordOutDelimiter[b]));
            }

            logger.debug("End of line delimeter " + bytes.toString());

            if (csv != null)
            {
                final StringBuilder csvMsg = new StringBuilder();
                csvMsg.append("csv: ");
                if (csv.header)
                    csvMsg.append("has header");
                else
                    csvMsg.append("no header");
                csvMsg.append(" quote=" + ByteCLA.asLiteral(csv.quoteByte));
                csvMsg.append(" separator=" + ByteCLA.asLiteral(csv.separatorByte));
                logger.debug(csvMsg.toString());
            }
        }

        logger.debug("power   = " + depth);

        // if (duplicateDisposition != DuplicateDisposition.Original)
        logger.info("dups    = " + duplicateDisposition.name());

        for (final String colName : columnHelper.getNames())
        {
            final KeyPart col = columnHelper.get(colName);
            if (csv == null)
                logger.debug("col \""
                        + col.columnName
                        + "\" "
                        + col.typeName
                        + " offset "
                        + col.offset
                        + " length "
                        + col.length
                        + (col.parseFormat == null
                                ? ""
                                : " format " + col.parseFormat));
            else
                logger.debug("col "
                        + col.columnName
                        + " "
                        + col.typeName
                        + " csvField "
                        + col.csvFieldNumber
                        + (col.parseFormat == null
                                ? ""
                                : " format " + col.parseFormat));
        }

        if (whereClause != null)
        {
            logger.info("where \"" + whereClause + "\"");
            try
            {
                logger.trace("\n" + Equ.getInstance().showRPN());
            } catch (final Exception e)
            {
                logger.warn("algebrain", e);
            }
        }

        int kno = 1;
        if (keys == null)
            logger.info("process = " + copyOrder.name() + " order");
        else
            for (final KeyPart def : keys)
            {
                if (csv == null)
                    logger.debug("key "
                            + (kno++)
                            + " "
                            + def.typeName
                            + " offset "
                            + def.offset
                            + " length "
                            + def.length
                            + " "
                            + def.direction.name()
                            + (def.parseFormat == null
                                    ? ""
                                    : " format " + def.parseFormat));
                else
                    logger.debug("key "
                            + (kno++)
                            + ") "
                            + def.typeName
                            + " csvField "
                            + def.csvFieldNumber
                            + " "
                            + def.direction.name()
                            + (def.parseFormat == null
                                    ? ""
                                    : " format " + def.parseFormat));
            }
    }

    /**
     * @param _logDirectory
     */
    private void startLogging (
            final File _logDirectory)
    {
        /*
         * Loggers will be defined only if funnel is being included in another
         * application that also has Log4J initialized. In this case we want to
         * use the LogManager initialize from the surrounding application.
         */
        if (LogManager.getCurrentLoggers().hasMoreElements())
            return;

        if (_logDirectory.mkdirs())
            System.out.println("mkdirs " + _logDirectory.getAbsolutePath());
        else if (!_logDirectory.exists())
            System.out.println("mkdirs, " + _logDirectory.getAbsolutePath() + " not created");

        final Properties log4j = new Properties();

        if (debug)
            log4j.setProperty("log4j.rootLogger", "DEBUG, " + MASTER_LOG_NAME);
        else
            log4j.setProperty("log4j.rootLogger", "INFO, " + MASTER_LOG_NAME);

        log4j.setProperty("log4j.appender." + MASTER_LOG_NAME, "org.apache.log4j.RollingFileAppender");
        log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".File", _logDirectory.getAbsolutePath()
                + "/funnel.log");
        log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".maxBackupIndex", "3");
        log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".maxFileSize", "10MB");

        log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".layout", "org.apache.log4j.PatternLayout");
        if (debug)
            log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".layout.ConversionPattern",
                    "%d%6r%5L-%-36c{1}%-6p%m%n");
        else
            log4j.setProperty("log4j.appender." + MASTER_LOG_NAME + ".layout.ConversionPattern", "%d %m%n");

        PropertyConfigurator.configure(log4j);
    }

    public boolean startNextInput ()
    {
        /*
         * Has the last input file been read? Then return false.
         */
        if (inputFileIndex() >= (inputFileCount() - 1))
            return false;
        inputFileIndex++;
        return true;
    }
}