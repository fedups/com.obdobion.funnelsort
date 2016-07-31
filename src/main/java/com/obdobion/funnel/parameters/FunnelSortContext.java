package com.obdobion.funnel.parameters;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import com.obdobion.algebrain.Equ;
import com.obdobion.argument.CmdLine;
import com.obdobion.argument.CmdLineCLA;
import com.obdobion.argument.ICmdLine;
import com.obdobion.argument.ICmdLineArg;
import com.obdobion.argument.WildFiles;
import com.obdobion.argument.annotation.Arg;
import com.obdobion.argument.input.CommandLineParser;
import com.obdobion.argument.input.IParserInput;
import com.obdobion.funnel.aggregation.Aggregate;
import com.obdobion.funnel.columns.FormatPart;
import com.obdobion.funnel.orderby.KeyPart;

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
            factoryArgName = "--type",
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
            factoryArgName = "--type",
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
            defaultValues = { "cr", "lf" },
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

    @Arg(help = "The definition of the CSV file being read as input.  Using this indicates that the input is in fact a CSV file and the columns parameter must use the --field arguments.")
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

    public FunnelSortContext(final ICmdLine specializedParser, final String... args) throws ParseException, IOException
    {
        commandLineParser = specializedParser;
        final IParserInput userInput = CommandLineParser.getInstance(commandLineParser.getCommandPrefix(), args);
        commandLineParser.parse(userInput, this);
        // push();
    }

    public FunnelSortContext(final String... args) throws ParseException, IOException
    {
        this(new CmdLine("FunnelSort", "Funnel is a sort / copy / merge utility ", '-', '!'), args);
    }

    public ICmdLine getParser()
    {
        return commandLineParser;
    }

    @SuppressWarnings("unchecked")
    public void push() throws ParseException
    {
        try
        {
            final ICmdLine cmdline0 = getParser();
            {
                final ICmdLineArg<com.obdobion.argument.WildFiles> arg0 = (ICmdLineArg<com.obdobion.argument.WildFiles>) (cmdline0
                        .arg("--inputFile"));
                arg0.reset();
                if (inputFiles != null)
                {
                    arg0.setValue(inputFiles);
                }
            }
            {
                final ICmdLineArg<java.io.File> arg0 = (ICmdLineArg<java.io.File>) (cmdline0.arg("-o"));
                arg0.reset();
                if (outputFile != null)
                {
                    arg0.setValue(outputFile);
                }
            }
            {
                final ICmdLineArg<java.lang.Boolean> arg0 = (ICmdLineArg<java.lang.Boolean>) (cmdline0.arg("-r"));
                arg0.reset();
                arg0.setValue(inPlaceSort);
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--headerIn"));
                arg0.reset();
                if (headerInDefs != null)
                {
                    for (final com.obdobion.funnel.orderby.KeyPart oneValue0 : headerInDefs)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-n"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-t"));
                            arg1.reset();
                            if (oneValue0.typeName != null)
                            {
                                arg1.setValue(oneValue0.typeName.name());
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-o"));
                            arg1.reset();
                            arg1.setValue(oneValue0.offset);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-l"));
                            arg1.reset();
                            arg1.setValue(oneValue0.length);
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-d"));
                            arg1.reset();
                            if (oneValue0.parseFormat != null)
                            {
                                arg1.setValue(oneValue0.parseFormat);
                            }
                        }
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--headerOut"));
                arg0.reset();
                if (headerOutDefs != null)
                {
                    for (final com.obdobion.funnel.columns.FormatPart oneValue0 : headerOutDefs)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("--columnName"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-t"));
                            arg1.reset();
                            if (oneValue0.typeName != null)
                            {
                                arg1.setValue(oneValue0.typeName.name());
                            }
                        }
                        {
                            final ICmdLineArg<com.obdobion.algebrain.Equ> arg1 = (ICmdLineArg<com.obdobion.algebrain.Equ>) (cmdline1
                                    .arg("-e"));
                            arg1.reset();
                            if (oneValue0.equation != null)
                            {
                                arg1.setValue(oneValue0.equation);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-d"));
                            arg1.reset();
                            if (oneValue0.format != null)
                            {
                                arg1.setValue(oneValue0.format);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-l"));
                            arg1.reset();
                            arg1.setValue(oneValue0.length);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-s"));
                            arg1.reset();
                            arg1.setValue(oneValue0.size);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-o"));
                            arg1.reset();
                            arg1.setValue(oneValue0.offset);
                        }
                        {
                            final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-f"));
                            arg1.reset();
                            arg1.setValue(oneValue0.filler);
                        }
                    }
                }
            }
            {
                final ICmdLineArg<java.lang.Integer> arg0 = (ICmdLineArg<java.lang.Integer>) (cmdline0
                        .arg("--fixedIn"));
                arg0.reset();
                arg0.setValue(fixedRecordLengthIn);
            }
            {
                final ICmdLineArg<java.lang.Integer> arg0 = (ICmdLineArg<java.lang.Integer>) (cmdline0
                        .arg("--fixedOut"));
                arg0.reset();
                arg0.setValue(fixedRecordLengthOut);
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--columnsIn"));
                arg0.reset();
                if (inputColumnDefs != null)
                {
                    for (final com.obdobion.funnel.orderby.KeyPart oneValue0 : inputColumnDefs)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-n"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-t"));
                            arg1.reset();
                            if (oneValue0.typeName != null)
                            {
                                arg1.setValue(oneValue0.typeName.name());
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-f"));
                            arg1.reset();
                            arg1.setValue(oneValue0.csvFieldNumber);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-o"));
                            arg1.reset();
                            arg1.setValue(oneValue0.offset);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-l"));
                            arg1.reset();
                            arg1.setValue(oneValue0.length);
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-d"));
                            arg1.reset();
                            if (oneValue0.parseFormat != null)
                            {
                                arg1.setValue(oneValue0.parseFormat);
                            }
                        }
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--formatOut"));
                arg0.reset();
                if (formatOutDefs != null)
                {
                    for (final com.obdobion.funnel.columns.FormatPart oneValue0 : formatOutDefs)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("--columnName"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                        {
                            final ICmdLineArg<com.obdobion.algebrain.Equ> arg1 = (ICmdLineArg<com.obdobion.algebrain.Equ>) (cmdline1
                                    .arg("-e"));
                            arg1.reset();
                            if (oneValue0.equation != null)
                            {
                                arg1.setValue(oneValue0.equation);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-t"));
                            arg1.reset();
                            if (oneValue0.typeName != null)
                            {
                                arg1.setValue(oneValue0.typeName.name());
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-d"));
                            arg1.reset();
                            if (oneValue0.format != null)
                            {
                                arg1.setValue(oneValue0.format);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-l"));
                            arg1.reset();
                            arg1.setValue(oneValue0.length);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-s"));
                            arg1.reset();
                            arg1.setValue(oneValue0.size);
                        }
                        {
                            final ICmdLineArg<java.lang.Integer> arg1 = (ICmdLineArg<java.lang.Integer>) (cmdline1
                                    .arg("-o"));
                            arg1.reset();
                            arg1.setValue(oneValue0.offset);
                        }
                        {
                            final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-f"));
                            arg1.reset();
                            arg1.setValue(oneValue0.filler);
                        }
                    }
                }
            }
            {
                final ICmdLineArg<java.lang.Byte> arg0 = (ICmdLineArg<java.lang.Byte>) (cmdline0
                        .arg("--variableOutput"));
                arg0.reset();
                if (endOfRecordDelimiterOut != null)
                {
                    for (int i0 = 0; i0 < endOfRecordDelimiterOut.length && i0 < arg0.getMultipleMax(); i0++)
                        arg0.setValue(endOfRecordDelimiterOut[i0]);
                }
            }
            {
                final ICmdLineArg<com.obdobion.algebrain.Equ> arg0 = (ICmdLineArg<com.obdobion.algebrain.Equ>) (cmdline0
                        .arg("-w"));
                arg0.reset();
                if (whereEqu != null)
                {
                    for (final com.obdobion.algebrain.Equ oneValue0 : whereEqu)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        arg0.setValue(oneValue0);
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.algebrain.Equ> arg0 = (ICmdLineArg<com.obdobion.algebrain.Equ>) (cmdline0
                        .arg("-s"));
                arg0.reset();
                if (stopEqu != null)
                {
                    for (final com.obdobion.algebrain.Equ oneValue0 : stopEqu)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        arg0.setValue(oneValue0);
                    }
                }
            }
            {
                final ICmdLineArg<java.lang.Byte> arg0 = (ICmdLineArg<java.lang.Byte>) (cmdline0
                        .arg("--variableInput"));
                arg0.reset();
                if (endOfRecordDelimiterIn != null)
                {
                    for (int i0 = 0; i0 < endOfRecordDelimiterIn.length && i0 < arg0.getMultipleMax(); i0++)
                        arg0.setValue(endOfRecordDelimiterIn[i0]);
                }
            }
            {
                final ICmdLineArg<java.lang.String> arg0 = (ICmdLineArg<java.lang.String>) (cmdline0.arg("-d"));
                arg0.reset();
                if (duplicateDisposition != null)
                {
                    arg0.setValue(duplicateDisposition.name());
                }
            }
            {
                final ICmdLineArg<java.lang.String> arg0 = (ICmdLineArg<java.lang.String>) (cmdline0.arg("-c"));
                arg0.reset();
                if (copyOrder != null)
                {
                    arg0.setValue(copyOrder.name());
                }
            }
            {
                final ICmdLineArg<java.lang.Long> arg0 = (ICmdLineArg<java.lang.Long>) (cmdline0.arg("--rowMax"));
                arg0.reset();
                arg0.setValue(maximumNumberOfRows);
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--orderBy"));
                arg0.reset();
                if (orderBys != null)
                {
                    for (final com.obdobion.funnel.parameters.OrderBy oneValue0 : orderBys)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("--columnName"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-d"));
                            arg1.reset();
                            if (oneValue0.direction != null)
                            {
                                arg1.setValue(oneValue0.direction.name());
                            }
                        }
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--hexDump"));
                arg0.reset();
                if (hexDumps != null)
                {
                    for (final com.obdobion.funnel.parameters.HexDump oneValue0 : hexDumps)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("--columnName"));
                            arg1.reset();
                            if (oneValue0.columnName != null)
                            {
                                arg1.setValue(oneValue0.columnName);
                            }
                        }
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--count"));
                arg0.reset();
                if (aggregates != null)
                {
                    for (final com.obdobion.funnel.aggregation.Aggregate oneValue0 : aggregates)
                    {
                        if (arg0.size() == arg0.getMultipleMax())
                            break;
                        final ICmdLine cmdline1 = (((CmdLineCLA) arg0).templateCmdLine).clone();
                        ((CmdLineCLA) arg0).setValue(cmdline1);
                        {
                            final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1
                                    .arg("-n"));
                            arg1.reset();
                            if (oneValue0.name != null)
                            {
                                arg1.setValue(oneValue0.name);
                            }
                        }
                    }
                }
            }
            {
                final ICmdLineArg<com.obdobion.argument.ICmdLine> arg0 = (ICmdLineArg<com.obdobion.argument.ICmdLine>) (cmdline0
                        .arg("--csv"));
                arg0.reset();
                if (csv != null)
                {
                    final ICmdLine cmdline1 = ((CmdLineCLA) arg0).templateCmdLine.clone();
                    {
                        final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1.arg("-f"));
                        arg1.reset();
                        if (csv.predefinedFormat != null)
                        {
                            arg1.setValue(csv.predefinedFormat.name());
                        }
                    }
                    {
                        final ICmdLineArg<java.lang.Boolean> arg1 = (ICmdLineArg<java.lang.Boolean>) (cmdline1
                                .arg("-h"));
                        arg1.reset();
                        arg1.setValue(csv.header);
                    }
                    {
                        final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-c"));
                        arg1.reset();
                        arg1.setValue(csv.commentMarker);
                    }
                    {
                        final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-d"));
                        arg1.reset();
                        arg1.setValue(csv.delimiter);
                    }
                    {
                        final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-x"));
                        arg1.reset();
                        arg1.setValue(csv.escape);
                    }
                    {
                        final ICmdLineArg<java.lang.Boolean> arg1 = (ICmdLineArg<java.lang.Boolean>) (cmdline1
                                .arg("-e"));
                        arg1.reset();
                        arg1.setValue(csv.ignoreEmptyLines);
                    }
                    {
                        final ICmdLineArg<java.lang.Boolean> arg1 = (ICmdLineArg<java.lang.Boolean>) (cmdline1
                                .arg("-s"));
                        arg1.reset();
                        arg1.setValue(csv.ignoreSurroundingSpaces);
                    }
                    {
                        final ICmdLineArg<java.lang.String> arg1 = (ICmdLineArg<java.lang.String>) (cmdline1.arg("-n"));
                        arg1.reset();
                        if (csv.nullString != null)
                        {
                            arg1.setValue(csv.nullString);
                        }
                    }
                    {
                        final ICmdLineArg<java.lang.Byte> arg1 = (ICmdLineArg<java.lang.Byte>) (cmdline1.arg("-q"));
                        arg1.reset();
                        arg1.setValue(csv.quote);
                    }
                }
            }
            {
                final ICmdLineArg<java.io.File> arg0 = (ICmdLineArg<java.io.File>) (cmdline0.arg("--workDirectory"));
                arg0.reset();
                if (workDirectory != null)
                {
                    arg0.setValue(workDirectory);
                }
            }
            {
                final ICmdLineArg<java.lang.Boolean> arg0 = (ICmdLineArg<java.lang.Boolean>) (cmdline0
                        .arg("--noCacheInput"));
                arg0.reset();
                arg0.setValue(noCacheInput);
            }
            {
                final ICmdLineArg<java.lang.Boolean> arg0 = (ICmdLineArg<java.lang.Boolean>) (cmdline0
                        .arg("--diskWork"));
                arg0.reset();
                arg0.setValue(diskWork);
            }
            {
                final ICmdLineArg<java.lang.Integer> arg0 = (ICmdLineArg<java.lang.Integer>) (cmdline0.arg("--power"));
                arg0.reset();
                arg0.setValue(depth);
            }
            {
                final ICmdLineArg<java.lang.Boolean> arg0 = (ICmdLineArg<java.lang.Boolean>) (cmdline0
                        .arg("--syntaxOnly"));
                arg0.reset();
                arg0.setValue(syntaxOnly);
            }
            {
                final ICmdLineArg<java.lang.Boolean> arg0 = (ICmdLineArg<java.lang.Boolean>) (cmdline0
                        .arg("--version"));
                arg0.reset();
                arg0.setValue(version);
            }
        } catch (final java.lang.Exception e)
        {
            throw new ParseException(e.getMessage(), 0);
        }
    }
}
