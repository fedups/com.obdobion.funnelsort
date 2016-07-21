package com.obdobion.funnel.parameters;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import com.obdobion.argument.CmdLine;
import com.obdobion.argument.CmdLineCLA;
import com.obdobion.argument.ICmdLine;
import com.obdobion.argument.ICmdLineArg;
import com.obdobion.argument.input.CommandLineParser;
import com.obdobion.argument.input.IParserInput;

/*-
FunnelSort

Funnel is a sort / copy / merge utility

    [positional <wildfiles>]
        The input file or files to be processed.  Wild cards are allowed in the
        filename only and the path (** indicates multiple path segments).
        Sysin is assumed if this parameter is not provided.
        Although no value is required, if you specify one you must specify at
        least 1.
        Upper vs lower case matters.

    [-o --outputFileName --OFN <file>]
        The output file to be written.  Sysout is assumed if this parameter is
        not provided.  The same name as the input file is allowed.
        Upper vs lower case matters.

    [-r --replace]
        Replace the input file with the results.

    [--headerIn --HI ()]
        Column definitions defining the file header layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        [-n --name <string>]
            A name for this column / key so that it can be referenced.

        positional <enum>
            The data type of the key in the file.

        [-o --offset <integer>]
            The zero relative offset from the beginning of a row.  This will be
            computed, if not specified, to be the location of the previous
            column plus the length of the previous column.  Most often this
            parameter is not needed.
            The value must be at least 0.
            If unspecified, the default will be -1.

        [-l --length <integer>]
            The length of the key in bytes.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-d --format <string>]
            The parsing format for converting the contents of the key in the
            file to an internal representation. Use Java SimpleDateFormat rules
            for making the format.
            Upper vs lower case matters.

    [--headerOut --HO ()]
        Column references defining the output file header layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        [positional <enum>]
            The data type to be written.  Defaults to the columnIn data type.
            Upper vs lower case matters.

        [-e --equation --EKXN <equ>]
            Used instead of a column name, this will be evaluated with the
            result written to the output.

        [-d --format <string>]
            The format for converting the contents of the data to be written.
            Use Java Formatter rules for making the format.  The format must
            match the type of the data.
            Upper vs lower case matters.

        [-l --length <integer>]
            The length of the key in bytes.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-s --size <integer>]
            The number of characters this field will use on output.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-o --offset <integer>]
            The zero relative offset from the beginning of a row.  This will be
            computed, if not specified, to be the location of the previous
            column plus the length of the previous column.  Most often this
            parameter is not needed.
            The value must be at least 0.
            If unspecified, the default will be -1.

        [-f --filler <byte>]
            The trailing filler character to use for a short field.

    [--fixedIn --FI <integer>]
        The record length in a fixed record length file.
        The value must be from 1 to 4096 inclusive

    [--fixedOut <integer>]
        The record length in a fixed record length file.  This is used to
        change an output file into a fixed format.  It is not necessary if
        --fixedIn is specified.
        The value must be from 1 to 4096 inclusive

    [--columnsIn --CI ()]
        Column definitions defining the input file layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        [-n --name <string>]
            A name for this column / key so that it can be referenced.
            Upper vs lower case matters.

        positional <enum>

            Upper vs lower case matters.

        [-f --field <integer>]
            If this is a CSV file then use this instead of offset and length.
            The first field is field #1 (not zero).
            The value must be at least 1.

        [-o --offset <integer>]
            The zero relative offset from the beginning of a row.  This will be
            computed, if not specified, to be the location of the previous
            column plus the length of the previous column.  Most often this
            parameter is not needed.
            The value must be at least 0.
            If unspecified, the default will be -1.

        [-l --length <integer>]
            The length of the key in bytes.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-d --format <string>]
            The parsing format for converting the contents of the key in the
            file to an internal representation. Use Java SimpleDateFormat rules
            for making the format.
            Upper vs lower case matters.

    [--formatOut --FO ()]
        Column references defining the output file layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        [-e --equation --EKXN <equ>]
            Used instead of a column name, this will be evaluated with the
            result written to the output.

        [positional <enum>]
            The data type to be written.  Defaults to the columnIn data type.

        [-d --format <string>]
            The format for converting the contents of the data to be written.
            Use Java Formatter rules for making the format.  The format must
            match the type of the data.
            Upper vs lower case matters.

        [-l --length <integer>]
            The length of the key in bytes.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-s --size <integer>]
            The number of characters this field will use on output.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-o --offset <integer>]
            The zero relative offset from the beginning of a row.  This will be
            computed, if not specified, to be the location of the previous
            column plus the length of the previous column.  Most often this
            parameter is not needed.
            The value must be at least 0.
            If unspecified, the default will be -1.

        [-f --filler <byte>]
            The trailing filler character to use for a short field.

    [--variableOutput --VO <byte>]
        The byte(s) that end each line in a variable length record file.  This
        will be used to write the output file as a variable length file.  If
        this is not specified then the --variableInput value will be used.
        Although no value is required, if you specify one you must specify at
        least 1.

    [-w --where --WR <equ>]
        Rows that evaluate to TRUE are selected for Output.  See
        \\\"Algebrain\\\" for details.  Columns are used as variables in this
        Algebrain equation.
        Although no value is required, if you specify one you must specify at
        least 1.

    [-s --stopWhen --SW --STPH <equ>]
        The sort will stop reading input when this equation returns TRUE.  See
        \\\"Algebrain\\\" for details.  Columns are used as variables in this
        Algebrain equation.
        Although no value is required, if you specify one you must specify at
        least 1.

    [--variableInput --VI <byte>]
        The byte(s) that end each line in a variable length record file.
        Although no value is required, if you specify one you must specify at
        least 1.
        If unspecified, the default will be cr(13) lf(10).

    [-d --duplicate <enum>]
        Special handling of duplicate keyed rows.
        If unspecified, the default will be original.

    [-c --copy <enum>]
        Defines the process that will take place on the input.
        If unspecified, the default will be bykey.

    [--rowMax --RM <long>]
        Used for variable length input, estimate the number of rows.  Too low
        could cause problems.
        The value must be at least 2.
        If unspecified, the default will be 9223372036854775807.

    [--orderBy --OB ()]
        The sort keys defined from columns.

        Although no value is required, if you specify one you must specify at
        least 1.
        positional <string>
            A previously defined column name.

        [positional <enum>]
            The direction of the sort for this key. AASC and ADESC are absolute
            values of the key - the case of letters would not matter and the
            sign of numbers would not matter.
            If unspecified, the default will be asc.

    [--hexDump --HD ()]
        Columns that will be shown in hex format.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

    [--count ()]
        Count the number of records per unique sort key

        Although no value is required, if you specify one you must specify at
        least 1.
        -n --name <string>
            A name for this aggregate so that it can be referenced.

    [--avg ()]
        A list of columns that will be analyzed for their respective average
        values per unique sort key.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        -n --name <string>
            A name for this aggregate so that it can be referenced.

        [-e --equation --EKXN <equ>]
            Used instead of a column name.

    [--max ()]
        A list of columns that will be analyzed for their respective maximum
        values per unique sort key.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        -n --name <string>
            A name for this aggregate so that it can be referenced.

        [-e --equation --EKXN <equ>]
            Used instead of a column name.

    [--min ()]
        A list of columns that will be analyzed for their respective minimum
        values per unique sort key.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        -n --name <string>
            A name for this aggregate so that it can be referenced.

        [-e --equation --EKXN <equ>]
            Used instead of a column name.

    [--sum ()]
        A list of columns that will be analyzed for their respective summary
        values per unique sort key.

        Although no value is required, if you specify one you must specify at
        least 1.
        [positional <string>]
            A previously defined column name.

        -n --name <string>
            A name for this aggregate so that it can be referenced.

        [-e --equation --EKXN <equ>]
            Used instead of a column name.

    [--csv ()]
        The definition of the CSV file being read as input.  Using this
        indicates that the input is in fact a CSV file and the columns
        parameter must use the --field arguments.

        [positional <enum>]
            A predefined way to parse the CSV input.  Other parameters may
            override the specifics of this definition.
            Upper vs lower case matters.
            If unspecified, the default will be Default.

        [-h --header]
            Skip over the first line for sorting and just write it to the
            beginning of the output file.

        [-c --commentMarker --CM <byte>]
            Sets the comment start marker of the format to the specified
            character. Note that the comment start character is only recognized
            at the start of a line.

        [-d --delimiter <byte>]
            Sets the delimiter of the format to the specified character.

        [-x --escape <byte>]
            Sets the escape character of the format to the specified character.

        [-e --ignoreEmptyLines --IEL]
            Sets the empty line skipping behavior of the format to true.

        [-s --ignoreSurroundingSpaces --ISS]
            Sets the trimming behavior of the format to true.

        [-n --nullString --NS <string>]
            Converts strings equal to the given nullString to null when reading
            records.

        [-q --quote <byte>]
            Sets the quoteChar of the format to the specified character.

    [--workDirectory --WD <file>]
        The directory where temp files will be handled.
        Upper vs lower case matters.

    [--noCacheInput --NCI]
        Caching the input file into memory is faster.  This will turn off the
        feature.

    [--diskWork --DW]
        Work files are stored on disk.  The amount of memory required to hold
        work areas in memory is about (2 * (keySize + 24)).

    [--power <integer>]
        The depth of the funnel.  The bigger this number is, the more memory
        will be used.  This is computed when --max or -f is specified.
        The value must be from 2 to 16 inclusive
        If unspecified, the default will be 16.

    [--syntaxOnly --SO]
        Check the command - will not run

    [--version]
        Display the version of FunnelSort
*/
public class FunnelSortContext
{
    private final ICmdLine                                     commandLineParser;
    /**
     * The input file or files to be processed. Wild cards are allowed in the
     * filename only and the path (** indicates multiple path segments). Sysin
     * is assumed if this parameter is not provided.
     */
    public com.obdobion.argument.WildFiles                     inputFiles;
    /**
     * The output file to be written. Sysout is assumed if this parameter is not
     * provided. The same name as the input file is allowed.
     */
    public java.io.File                                        outputFile;
    /**
     * Replace the input file with the results.
     */
    public boolean                                             inPlaceSort;
    /**
     * Column definitions defining the file header layout.
     */
    public List<com.obdobion.funnel.orderby.KeyPart>           headerInDefs;
    /**
     * Column references defining the output file header layout.
     */
    public List<com.obdobion.funnel.columns.FormatPart>        headerOutDefs;
    /**
     * The record length in a fixed record length file.
     */
    public int                                                 fixedRecordLengthIn;
    /**
     * The record length in a fixed record length file. This is used to change
     * an output file into a fixed format. It is not necessary if --fixedIn is
     * specified.
     */
    public int                                                 fixedRecordLengthOut;
    /**
     * Column definitions defining the input file layout.
     */
    public List<com.obdobion.funnel.orderby.KeyPart>           inputColumnDefs;
    /**
     * Column references defining the output file layout.
     */
    public List<com.obdobion.funnel.columns.FormatPart>        formatOutDefs;
    /**
     * The byte(s) that end each line in a variable length record file. This
     * will be used to write the output file as a variable length file. If this
     * is not specified then the --variableInput value will be used.
     */
    public byte[]                                              endOfRecordDelimiterOut;
    /**
     * Rows that evaluate to TRUE are selected for Output. See \\\"Algebrain\\\"
     * for details. Columns are used as variables in this Algebrain equation.
     */
    public List<com.obdobion.algebrain.Equ>                    whereEqu;
    /**
     * The sort will stop reading input when this equation returns TRUE. See
     * \\\"Algebrain\\\" for details. Columns are used as variables in this
     * Algebrain equation.
     */
    public List<com.obdobion.algebrain.Equ>                    stopEqu;
    /**
     * The byte(s) that end each line in a variable length record file.
     */
    public byte[]                                              endOfRecordDelimiterIn;
    /**
     * Special handling of duplicate keyed rows.
     */
    public com.obdobion.funnel.parameters.DuplicateDisposition duplicateDisposition;
    /**
     * Defines the process that will take place on the input.
     */
    public com.obdobion.funnel.parameters.CopyOrder            copyOrder;
    /**
     * Used for variable length input, estimate the number of rows. Too low
     * could cause problems.
     */
    public long                                                maximumNumberOfRows;
    /**
     * The sort keys defined from columns.
     */
    public List<com.obdobion.funnel.parameters.OrderBy>        orderBys;
    /**
     * Columns that will be shown in hex format.
     */
    public List<com.obdobion.funnel.parameters.HexDump>        hexDumps;
    /**
     * Count the number of records per unique sort key
     */
    public List<com.obdobion.funnel.aggregation.Aggregate>     aggregates;
    /**
     * The definition of the CSV file being read as input. Using this indicates
     * that the input is in fact a CSV file and the columns parameter must use
     * the --field arguments.
     */
    public com.obdobion.funnel.parameters.CSVDef               csv;
    /**
     * The directory where temp files will be handled.
     */
    public java.io.File                                        workDirectory;
    /**
     * Caching the input file into memory is faster. This will turn off the
     * feature.
     */
    public boolean                                             noCacheInput;
    /**
     * Work files are stored on disk. The amount of memory required to hold work
     * areas in memory is about (2 * (keySize + 24)).
     */
    public boolean                                             diskWork;
    /**
     * The depth of the funnel. The bigger this number is, the more memory will
     * be used. This is computed when --max or -f is specified.
     */
    public int                                                 depth;
    /**
     * Check the command - will not run
     */
    public boolean                                             syntaxOnly;
    /**
     * Display the version of FunnelSort
     */
    public boolean                                             version;

    public FunnelSortContext(final ICmdLine specializedParser, final String... args) throws ParseException, IOException
    {
        commandLineParser = specializedParser;
        commandLineParser.compile(new String[]
        {
                "--type wildfile--uid 1 -k 'inputFileName' -p -c --camelcaps -h 'The input file or files to be processed.  Wild cards are allowed in the filename only and the path (** indicates multiple path segments).  Sysin is assumed if this parameter is not provided.' -v 'inputFiles' -m '1'",
                "--type file--uid 2 -k o 'outputFileName' -c --camelcaps -h 'The output file to be written.  Sysout is assumed if this parameter is not provided.  The same name as the input file is allowed.' -v 'outputFile'",
                "--type boolean--uid 3 -k r 'replace' -h 'Replace the input file with the results.' -v 'inPlaceSort'",
                "--type begin--uid 4 -k 'headerIn' --camelcaps -h 'Column definitions defining the file header layout.' --class 'com.obdobion.funnel.orderby.KeyPart' --factoryMethod 'com.obdobion.funnel.orderby.KeyType.create' --factoryArgName '--type' -v 'headerInDefs' -m '1'",
                "--type string--uid 5 -k n 'name' -h 'A name for this column / key so that it can be referenced.' -v 'columnName'",
                "--type enum--uid 6 -k t 'type' -p -r -h 'The data type of the key in the file.' --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type integer--uid 7 -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type integer--uid 8 -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type string--uid 9 -k d 'format' -c -h 'The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.' -v 'parseFormat'",
                "--type end -k headerIn",
                "--type begin--uid 11 -k 'headerOut' --camelcaps -h 'Column references defining the output file header layout.' --class 'com.obdobion.funnel.columns.FormatPart' -v 'headerOutDefs' -m '1'",
                "--type string--uid 12 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type enum--uid 13 -k t 'type' -p -c -h 'The data type to be written.  Defaults to the columnIn data type.' -v 'typeName'",
                "--type equ--uid 14 -k e 'equation' --metaphone -h 'Used instead of a column name, this will be evaluated with the result written to the output.' -v 'equation'",
                "--type string--uid 15 -k d 'format' -c -h 'The format for converting the contents of the data to be written. Use Java Formatter rules for making the format.  The format must match the type of the data.' -v 'format'",
                "--type integer--uid 16 -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type integer--uid 17 -k s 'size' -h 'The number of characters this field will use on output.' -d '255' -v 'size' --range '1' '255'",
                "--type integer--uid 18 -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type byte--uid 19 -k f 'filler' -h 'The trailing filler character to use for a short field.' -v 'filler'",
                "--type end -k headerOut",
                "--type integer--uid 21 -k 'fixedIn' --camelcaps -h 'The record length in a fixed record length file.' -v 'fixedRecordLengthIn' --range '1' '4096'",
                "--type integer--uid 22 -k 'fixedOut' -h 'The record length in a fixed record length file.  This is used to change an output file into a fixed format.  It is not necessary if --fixedIn is specified.' -v 'fixedRecordLengthOut' --range '1' '4096'",
                "--type begin--uid 23 -k 'columnsIn' --camelcaps -h 'Column definitions defining the input file layout.' --class 'com.obdobion.funnel.orderby.KeyPart' --factoryMethod 'com.obdobion.funnel.orderby.KeyType.create' --factoryArgName '--type' -v 'inputColumnDefs' -m '1'",
                "--type string--uid 24 -k n 'name' -c -h 'A name for this column / key so that it can be referenced.' -v 'columnName'",
                "--type enum--uid 25 -k t 'type' -p -r -c --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type integer--uid 26 -k f 'field' -h 'If this is a CSV file then use this instead of offset and length.  The first field is field #1 (not zero).' -v 'csvFieldNumber' --range '1'",
                "--type integer--uid 27 -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type integer--uid 28 -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type string--uid 29 -k d 'format' -c -h 'The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.' -v 'parseFormat'",
                "--type end -k columnsIn",
                "--type begin--uid 31 -k 'formatOut' --camelcaps -h 'Column references defining the output file layout.' --class 'com.obdobion.funnel.columns.FormatPart' -v 'formatOutDefs' -m '1'",
                "--type string--uid 32 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type equ--uid 33 -k e 'equation' --metaphone -h 'Used instead of a column name, this will be evaluated with the result written to the output.' -v 'equation'",
                "--type enum--uid 34 -k t 'type' -p -h 'The data type to be written.  Defaults to the columnIn data type.' --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type string--uid 35 -k d 'format' -c -h 'The format for converting the contents of the data to be written. Use Java Formatter rules for making the format.  The format must match the type of the data.' -v 'format'",
                "--type integer--uid 36 -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type integer--uid 37 -k s 'size' -h 'The number of characters this field will use on output.' -d '255' -v 'size' --range '1' '255'",
                "--type integer--uid 38 -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type byte--uid 39 -k f 'filler' -h 'The trailing filler character to use for a short field.' -v 'filler'",
                "--type end -k formatOut",
                "--type byte--uid 41 -k 'variableOutput' --camelcaps -h 'The byte(s) that end each line in a variable length record file.  This will be used to write the output file as a variable length file.  If this is not specified then the --variableInput value will be used.' -v 'endOfRecordDelimiterOut' -m '1'",
                "--type equ--uid 42 -k w 'where' --metaphone -h 'Rows that evaluate to TRUE are selected for Output.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.' -v 'whereEqu' -m '1'",
                "--type equ--uid 43 -k s 'stopWhen' --camelcaps --metaphone -h 'The sort will stop reading input when this equation returns TRUE.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.' -v 'stopEqu' -m '1'",
                "--type byte--uid 44 -k 'variableInput' --camelcaps -h 'The byte(s) that end each line in a variable length record file.' -d cr lf -v 'endOfRecordDelimiterIn' -m '1'",
                "--type enum--uid 45 -k d 'duplicate' -h 'Special handling of duplicate keyed rows.' -d 'original' --class 'com.obdobion.funnel.parameters.DuplicateDisposition' -v 'duplicateDisposition'",
                "--type enum--uid 46 -k c 'copy' -h 'Defines the process that will take place on the input.' -d 'bykey' --class 'com.obdobion.funnel.parameters.CopyOrder' -v 'copyOrder'",
                "--type long--uid 47 -k 'rowMax' --camelcaps -h 'Used for variable length input, estimate the number of rows.  Too low could cause problems.' -d '9223372036854775807' -v 'maximumNumberOfRows' --range '2'",
                "--type begin--uid 48 -k 'orderBy' --camelcaps -h 'The sort keys defined from columns.' --class 'com.obdobion.funnel.parameters.OrderBy' -v 'orderBys' -m '1'",
                "--type string--uid 49 -k 'columnName' -p -r --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type enum--uid 50 -k d 'direction' -p -h 'The direction of the sort for this key. AASC and ADESC are absolute values of the key - the case of letters would not matter and the sign of numbers would not matter.' -d 'asc' --class 'com.obdobion.funnel.orderby.KeyDirection' -v 'direction'",
                "--type end -k orderBy",
                "--type begin--uid 52 -k 'hexDump' --camelcaps -h 'Columns that will be shown in hex format.' --class 'com.obdobion.funnel.parameters.HexDump' -v 'hexDumps' -m '1'",
                "--type string--uid 53 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type end -k hexDump",
                "--type begin--uid 55 -k 'count' -h 'Count the number of records per unique sort key' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newCount' -v 'aggregates' -m '1'",
                "--type string--uid 56 -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type end -k count",
                "--type begin--uid 58 -k 'avg' -h 'A list of columns that will be analyzed for their respective average values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newAvg' -v 'aggregates' -m '1'",
                "--type string--uid 59 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string--uid 60 -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ--uid 61 -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k avg",
                "--type begin--uid 62 -k 'max' -h 'A list of columns that will be analyzed for their respective maximum values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newMax' -v 'aggregates' -m '1'",
                "--type string--uid 63 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string--uid 64 -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ--uid 65 -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k max",
                "--type begin--uid 67 -k 'min' -h 'A list of columns that will be analyzed for their respective minimum values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newMin' -v 'aggregates' -m '1'",
                "--type string--uid 68 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string--uid 69 -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ--uid 70 -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k min",
                "--type begin--uid 72 -k 'sum' -h 'A list of columns that will be analyzed for their respective summary values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newSum' -v 'aggregates' -m '1'",
                "--type string--uid 73 -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string--uid 74 -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ--uid 75 -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k sum",
                "--type begin--uid 77 -k 'csv' -h 'The definition of the CSV file being read as input.  Using this indicates that the input is in fact a CSV file and the columns parameter must use the --field arguments.' --class 'com.obdobion.funnel.parameters.CSVDef' -v 'csv'",
                "--type enum--uid 78 -k f -p -c -h 'A predefined way to parse the CSV input.  Other parameters may override the specifics of this definition.' -d 'Default' --class 'org.apache.commons.csv.CSVFormat' -v 'predefinedFormat' --enumlist 'org.apache.commons.csv.CSVFormat'",
                "--type boolean--uid 79 -k h 'header' -h 'Skip over the first line for sorting and just write it to the beginning of the output file.' -v 'header'",
                "--type byte--uid 80 -k c 'commentMarker' --camelcaps -h 'Sets the comment start marker of the format to the specified character. Note that the comment start character is only recognized at the start of a line.' -v 'commentMarker'",
                "--type byte--uid 81 -k d 'delimiter' -h 'Sets the delimiter of the format to the specified character.' -v 'delimiter'",
                "--type byte--uid 82 -k x 'escape' -h 'Sets the escape character of the format to the specified character.' -v 'escape'",
                "--type boolean--uid 83 -k e 'ignoreEmptyLines' --camelcaps -h 'Sets the empty line skipping behavior of the format to true.' -v 'ignoreEmptyLines'",
                "--type boolean--uid 84 -k s 'ignoreSurroundingSpaces' --camelcaps -h 'Sets the trimming behavior of the format to true.' -v 'ignoreSurroundingSpaces'",
                "--type string--uid 85 -k n 'nullString' --camelcaps -h 'Converts strings equal to the given nullString to null when reading records.' -v 'nullString'",
                "--type byte--uid 86 -k q 'quote' -h 'Sets the quoteChar of the format to the specified character.' -v 'quote'",
                "--type end -k csv",
                "--type file--uid 88 -k 'workDirectory' -c --camelcaps -h 'The directory where temp files will be handled.' -v 'workDirectory'",
                "--type boolean--uid 89 -k 'noCacheInput' --camelcaps -h 'Caching the input file into memory is faster.  This will turn off the feature.' -v 'noCacheInput'",
                "--type boolean--uid 90 -k 'diskWork' --camelcaps -h 'Work files are stored on disk.  The amount of memory required to hold work areas in memory is about (2 * (keySize + 24)).' -v 'diskWork'",
                "--type integer--uid 91 -k 'power' -h 'The depth of the funnel.  The bigger this number is, the more memory will be used.  This is computed when --max or -f is specified.' -d '16' -v 'depth' --range '2' '16'",
                "--type boolean--uid 92 -k 'syntaxOnly' --camelcaps -h 'Check the command - will not run' -v 'syntaxOnly'",
                "--type boolean--uid 93 -k 'version' -c -h 'Display the version of FunnelSort' -v 'version'"
        });
        final IParserInput userInput = CommandLineParser.getInstance(commandLineParser.getCommandPrefix(), args);
        commandLineParser.parse(userInput, this);
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
                        .arg("--inputFileName"));
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
