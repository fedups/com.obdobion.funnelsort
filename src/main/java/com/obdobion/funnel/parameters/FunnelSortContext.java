package com.obdobion.funnel.parameters;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import com.obdobion.algebrain.Equ;
import com.obdobion.argument.CmdLine;
import com.obdobion.argument.ICmdLine;
import com.obdobion.argument.annotation.Arg;
import com.obdobion.argument.input.CommandLineParser;
import com.obdobion.argument.input.IParserInput;
import com.obdobion.argument.type.WildFiles;
import com.obdobion.funnel.aggregation.Aggregate;
import com.obdobion.funnel.columns.FormatPart;
import com.obdobion.funnel.orderby.KeyPart;

/**
 * <p>
 * FunnelSortContext class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FunnelSortContext
{
    private final ICmdLine      commandLineParser;
    @Arg(longName = "inputFileName",
            positional = true,
            caseSensitive = true,
            allowCamelCaps = true,
            multimin = 1,
            help = "The input file or files to be processed.  Wild cards are allowed in the filename only and the path (** indicates multiple path segments).  Sysin is assumed if this parameter is not provided.")
    public WildFiles            inputFiles;

    @Arg(shortName = 'o',
            longName = "outputFileName",
            caseSensitive = true,
            allowCamelCaps = true,
            help = "The output file to be written.  Sysout is assumed if this parameter is not provided.  The same name as the input file is allowed.")
    public java.io.File         outputFile;

    @Arg(shortName = 'r', longName = "replace", help = "Replace the input file with the results.")
    public boolean              inPlaceSort;

    @Arg(longName = "headerIn",
            allowCamelCaps = true,
            help = "Column definitions defining the file header layout.",
            factoryMethod = "com.obdobion.funnel.orderby.KeyType.create",
            factoryArgName = "typeName",
            excludeArgs = { "csvFieldNumber", "direction" })
    public List<KeyPart>        headerInDefs;

    @Arg(longName = "headerOut",
            allowCamelCaps = true,
            help = "Column references defining the output file header layout.")
    public List<FormatPart>     headerOutDefs;

    @Arg(longName = "fixedIn",
            allowCamelCaps = true,
            range = { "1", "4096" },
            help = "The record length in a fixed record length file.")
    public int                  fixedRecordLengthIn;

    @Arg(longName = "fixedOut",
            range = { "1", "4096" },
            help = "The record length in a fixed record length file.  This is used to change an output file into a fixed format.  It is not necessary if --fixedIn is specified.")
    public int                  fixedRecordLengthOut;

    @Arg(longName = "columnsIn",
            allowCamelCaps = true,
            factoryMethod = "com.obdobion.funnel.orderby.KeyType.create",
            factoryArgName = "typeName",
            help = "Column definitions defining the input file layout.")
    public List<KeyPart>        inputColumnDefs;

    @Arg(longName = "formatOut", help = "Column references defining the output file layout.")
    public List<FormatPart>     formatOutDefs;

    @Arg(longName = "variableOutput",
            allowCamelCaps = true,
            help = "The byte(s) that end each line in a variable length record file.  This will be used to write the output file as a variable length file.  If this is not specified then the --variableInput value will be used.")
    public byte[]               endOfRecordDelimiterOut;

    @Arg(shortName = 'w',
            longName = "where",
            allowMetaphone = true,
            help = "Rows that evaluate to TRUE are selected for Output.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.")
    public List<Equ>            whereEqu;

    @Arg(shortName = 's',
            longName = "stopWhen",
            allowCamelCaps = true,
            allowMetaphone = true,
            help = "The sort will stop reading input when this equation returns TRUE.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.")
    public List<Equ>            stopEqu;

    @Arg(longName = "variableInput",
            allowCamelCaps = true,
            help = "The byte(s) that end each line in a variable length record file.")
    public byte[]               endOfRecordDelimiterIn;

    @Arg(shortName = 'd',
            longName = "duplicate",
            defaultValues = { "original" },
            help = "Special handling of duplicate keyed rows.")
    public DuplicateDisposition duplicateDisposition;

    @Arg(shortName = 'c',
            longName = "copy",
            defaultValues = { "byKey" },
            help = "Defines the process that will take place on the input.")
    public CopyOrder            copyOrder;

    @Arg(longName = "rowMax",
            allowCamelCaps = true,
            defaultValues = { "9223372036854775807" },
            range = { "2" },
            help = "Used for variable length input, estimate the number of rows.  Too low could cause problems.")
    public long                 maximumNumberOfRows;

    @Arg(longName = "orderBy", allowCamelCaps = true, help = "The sort keys defined from columns.")
    public List<OrderBy>        orderBys;

    @Arg(longName = "hexDump", allowCamelCaps = true, help = "Columns that will be shown in hex format.")
    public List<HexDump>        hexDumps;

    @Arg(longName = "count",
            factoryMethod = "newCount",
            help = "Count the number of records per unique sort key",
            multimin = 1,
            multimax = 1,
            excludeArgs = { "columnName", "equation" })
    @Arg(longName = "avg",
            factoryMethod = "newAvg",
            help = "A list of columns that will be analyzed for their respective average values per unique sort key.")
    @Arg(longName = "max",
            factoryMethod = "newMax",
            help = "A list of columns that will be analyzed for their respective maximum values per unique sort key.")
    @Arg(longName = "min",
            factoryMethod = "newMin",
            help = "A list of columns that will be analyzed for their respective minimum values per unique sort key.")
    @Arg(longName = "sum",
            factoryMethod = "newSum",
            help = "A list of columns that will be analyzed for their respective summary values per unique sort key.")
    public List<Aggregate>      aggregates;

    @Arg(
            help = "The definition of the CSV file being read as input.  Using this indicates that the input is in fact a CSV file and the columns parameter must use the --field arguments.")
    public CSVDef               csv;

    @Arg(allowCamelCaps = true, caseSensitive = true, help = "The directory where temp files will be handled.")
    public File                 workDirectory;

    @Arg(allowCamelCaps = true, help = "Caching the input file into memory is faster.  This will turn off the feature.")
    public boolean              noCacheInput;

    @Arg(allowCamelCaps = true,
            help = "Work files are stored on disk.  The amount of memory required to hold work areas in memory is about (2 * (keySize + 24)).")
    public boolean              diskWork;

    @Arg(longName = "power",
            defaultValues = "16",
            range = { "2", "16" },
            help = "The depth of the funnel.  The bigger this number is, the more memory will be used.  This is computed when --max or -f is specified.")
    public int                  depth;

    @Arg(allowCamelCaps = true, help = "Check the command - will not run")
    public boolean              syntaxOnly;

    @Arg(help = "Display the version of FunnelSort")
    public boolean              version;

    /**
     * <p>
     * Constructor for FunnelSortContext.
     * </p>
     *
     * @param parser a {@link com.obdobion.argument.CmdLine} object.
     * @param args a {@link java.lang.String} object.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public FunnelSortContext(final CmdLine parser, final String... args) throws ParseException, IOException
    {
        commandLineParser = parser;
        final IParserInput userInput = CommandLineParser.getInstance(commandLineParser.getCommandPrefix(), args);
        commandLineParser.parse(userInput, this);
    }

    /**
     * <p>
     * getParser.
     * </p>
     *
     * @return a {@link com.obdobion.argument.ICmdLine} object.
     */
    public ICmdLine getParser()
    {
        return commandLineParser;
    }
}
