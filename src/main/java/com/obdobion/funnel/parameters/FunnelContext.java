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
import com.obdobion.argument.ByteCLA;
import com.obdobion.argument.CmdLine;
import com.obdobion.argument.CmdLineCLA;
import com.obdobion.argument.ICmdLine;
import com.obdobion.argument.WildFiles;
import com.obdobion.argument.input.CommandLineParser;
import com.obdobion.argument.input.IParserInput;
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
 * @author Chris DeGreef
 *
 */
public class FunnelContext
{
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

        [--fixedOut --FO <integer>]
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
            Rows that evaluate to TRUE are selected for Output.  See "Algebrain"
            for details.  Columns are used as variables in this Algebrain equation.
            Although no value is required, if you specify one you must specify at
            least 1.

        [-s --stopWhen --SW --STPH <equ>]
            The sort will stop reading input when this equation returns TRUE.  See
            "Algebrain" for details.  Columns are used as variables in this
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
         * The input file or files to be processed. Wild cards are allowed in
         * the filename only and the path (** indicates multiple path segments).
         * Sysin is assumed if this parameter is not provided.
         */
        public com.obdobion.argument.WildFiles                     inputFiles;
        /**
         * The output file to be written. Sysout is assumed if this parameter is
         * not provided. The same name as the input file is allowed.
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
         * The record length in a fixed record length file. This is used to
         * change an output file into a fixed format. It is not necessary if
         * --fixedIn is specified.
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
         * will be used to write the output file as a variable length file. If
         * this is not specified then the --variableInput value will be used.
         */
        public byte[]                                              endOfRecordDelimiterOut;
        /**
         * Rows that evaluate to TRUE are selected for Output. See "Algebrain"
         * for details. Columns are used as variables in this Algebrain
         * equation.
         */
        public List<com.obdobion.algebrain.Equ>                    whereEqu;
        /**
         * The sort will stop reading input when this equation returns TRUE. See
         * "Algebrain" for details. Columns are used as variables in this
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
         * The definition of the CSV file being read as input. Using this
         * indicates that the input is in fact a CSV file and the columns
         * parameter must use the --field arguments.
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
         * Work files are stored on disk. The amount of memory required to hold
         * work areas in memory is about (2 * (keySize + 24)).
         */
        public boolean                                             diskWork;
        /**
         * The depth of the funnel. The bigger this number is, the more memory
         * will be used. This is computed when --max or -f is specified.
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

        public FunnelSortContext(final String[] args) throws ParseException, IOException
        {
            this(args, new CmdLine("FunnelSort", "Funnel is a sort / copy / merge utility", '-', '!'));
        }

        public FunnelSortContext(final String[] args, final ICmdLine specializedParser)
                throws ParseException,
                    IOException
        {
            commandLineParser = specializedParser;
            commandLineParser.compile(new String[]
            {
                "--type wildfile -k 'inputFileName' -p -c --camelcaps -h 'The input file or files to be processed.  Wild cards are allowed in the filename only and the path (** indicates multiple path segments).  Sysin is assumed if this parameter is not provided.' -v 'inputFiles' -m '1'",
                "--type file -k o 'outputFileName' -c --camelcaps -h 'The output file to be written.  Sysout is assumed if this parameter is not provided.  The same name as the input file is allowed.' -v 'outputFile'",
                "--type boolean -k r 'replace' -h 'Replace the input file with the results.' -v 'inPlaceSort'",
                "--type begin -k 'headerIn' --camelcaps -h 'Column definitions defining the file header layout.' --class 'com.obdobion.funnel.orderby.KeyPart' --factoryMethod 'com.obdobion.funnel.orderby.KeyType.create' --factoryArgName '--type' -v 'headerInDefs' -m '1'",
                "--type string -k n 'name' -h 'A name for this column / key so that it can be referenced.' -v 'columnName'",
                "--type enum -k t 'type' -p -r -h 'The data type of the key in the file.' --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type integer -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type integer -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type string -k d 'format' -c -h 'The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.' -v 'parseFormat'",
                "--type end -k headerIn",
                "--type begin -k 'headerOut' --camelcaps -h 'Column references defining the output file header layout.' --class 'com.obdobion.funnel.columns.FormatPart' -v 'headerOutDefs' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type enum -k t 'type' -p -c -h 'The data type to be written.  Defaults to the columnIn data type.' -v 'typeName'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name, this will be evaluated with the result written to the output.' -v 'equation'",
                "--type string -k d 'format' -c -h 'The format for converting the contents of the data to be written. Use Java Formatter rules for making the format.  The format must match the type of the data.' -v 'format'",
                "--type integer -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type integer -k s 'size' -h 'The number of characters this field will use on output.' -d '255' -v 'size' --range '1' '255'",
                "--type integer -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type byte -k f 'filler' -h 'The trailing filler character to use for a short field.' -v 'filler'",
                "--type end -k headerOut",
                "--type integer -k 'fixedIn' --camelcaps -h 'The record length in a fixed record length file.' -v 'fixedRecordLengthIn' --range '1' '4096'",
                "--type integer -k 'fixedOut' --camelcaps -h 'The record length in a fixed record length file.  This is used to change an output file into a fixed format.  It is not necessary if --fixedIn is specified.' -v 'fixedRecordLengthOut' --range '1' '4096'",
                "--type begin -k 'columnsIn' --camelcaps -h 'Column definitions defining the input file layout.' --class 'com.obdobion.funnel.orderby.KeyPart' --factoryMethod 'com.obdobion.funnel.orderby.KeyType.create' --factoryArgName '--type' -v 'inputColumnDefs' -m '1'",
                "--type string -k n 'name' -c -h 'A name for this column / key so that it can be referenced.' -v 'columnName'",
                "--type enum -k t 'type' -p -r -c --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type integer -k f 'field' -h 'If this is a CSV file then use this instead of offset and length.  The first field is field #1 (not zero).' -v 'csvFieldNumber' --range '1'",
                "--type integer -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type integer -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type string -k d 'format' -c -h 'The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.' -v 'parseFormat'",
                "--type end -k columnsIn",
                "--type begin -k 'formatOut' --camelcaps -h 'Column references defining the output file layout.' --class 'com.obdobion.funnel.columns.FormatPart' -v 'formatOutDefs' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name, this will be evaluated with the result written to the output.' -v 'equation'",
                "--type enum -k t 'type' -p -h 'The data type to be written.  Defaults to the columnIn data type.' --class 'com.obdobion.funnel.orderby.KeyType' -v 'typeName'",
                "--type string -k d 'format' -c -h 'The format for converting the contents of the data to be written. Use Java Formatter rules for making the format.  The format must match the type of the data.' -v 'format'",
                "--type integer -k l 'length' -h 'The length of the key in bytes.' -d '255' -v 'length' --range '1' '255'",
                "--type integer -k s 'size' -h 'The number of characters this field will use on output.' -d '255' -v 'size' --range '1' '255'",
                "--type integer -k o 'offset' -h 'The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.' -d '-1' -v 'offset' --range '0'",
                "--type byte -k f 'filler' -h 'The trailing filler character to use for a short field.' -v 'filler'",
                "--type end -k formatOut",
                "--type byte -k 'variableOutput' --camelcaps -h 'The byte(s) that end each line in a variable length record file.  This will be used to write the output file as a variable length file.  If this is not specified then the --variableInput value will be used.' -v 'endOfRecordDelimiterOut' -m '1'",
                "--type equ -k w 'where' --metaphone -h 'Rows that evaluate to TRUE are selected for Output.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.' -v 'whereEqu' -m '1'",
                "--type equ -k s 'stopWhen' --camelcaps --metaphone -h 'The sort will stop reading input when this equation returns TRUE.  See \"Algebrain\" for details.  Columns are used as variables in this Algebrain equation.' -v 'stopEqu' -m '1'",
                "--type byte -k 'variableInput' --camelcaps -h 'The byte(s) that end each line in a variable length record file.' -d cr lf -v 'endOfRecordDelimiterIn' -m '1'",
                "--type enum -k d 'duplicate' -h 'Special handling of duplicate keyed rows.' -d 'original' --class 'com.obdobion.funnel.parameters.DuplicateDisposition' -v 'duplicateDisposition'",
                "--type enum -k c 'copy' -h 'Defines the process that will take place on the input.' -d 'bykey' --class 'com.obdobion.funnel.parameters.CopyOrder' -v 'copyOrder'",
                "--type long -k 'rowMax' --camelcaps -h 'Used for variable length input, estimate the number of rows.  Too low could cause problems.' -d '9223372036854775807' -v 'maximumNumberOfRows' --range '2'",
                "--type begin -k 'orderBy' --camelcaps -h 'The sort keys defined from columns.' --class 'com.obdobion.funnel.parameters.OrderBy' -v 'orderBys' -m '1'",
                "--type string -k 'columnName' -p -r --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type enum -k d 'direction' -p -h 'The direction of the sort for this key. AASC and ADESC are absolute values of the key - the case of letters would not matter and the sign of numbers would not matter.' -d 'asc' --class 'com.obdobion.funnel.orderby.KeyDirection' -v 'direction'",
                "--type end -k orderBy",
                "--type begin -k 'hexDump' --camelcaps -h 'Columns that will be shown in hex format.' --class 'com.obdobion.funnel.parameters.HexDump' -v 'hexDumps' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type end -k hexDump",
                "--type begin -k 'count' -h 'Count the number of records per unique sort key' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newCount' -v 'aggregates' -m '1'",
                "--type string -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type end -k count",
                "--type begin -k 'avg' -h 'A list of columns that will be analyzed for their respective average values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newAvg' -v 'aggregates' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k avg",
                "--type begin -k 'max' -h 'A list of columns that will be analyzed for their respective maximum values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newMax' -v 'aggregates' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k max",
                "--type begin -k 'min' -h 'A list of columns that will be analyzed for their respective minimum values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newMin' -v 'aggregates' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k min",
                "--type begin -k 'sum' -h 'A list of columns that will be analyzed for their respective summary values per unique sort key.' --class 'com.obdobion.funnel.aggregation.Aggregate' --factoryMethod 'com.obdobion.funnel.aggregation.Aggregate.newSum' -v 'aggregates' -m '1'",
                "--type string -k 'columnName' -p --camelcaps -h 'A previously defined column name.' -v 'columnName'",
                "--type string -k n 'name' -r -h 'A name for this aggregate so that it can be referenced.' -v 'name'",
                "--type equ -k e 'equation' --metaphone -h 'Used instead of a column name.' -v 'equation'",
                "--type end -k sum",
                "--type begin -k 'csv' -h 'The definition of the CSV file being read as input.  Using this indicates that the input is in fact a CSV file and the columns parameter must use the --field arguments.' --class 'com.obdobion.funnel.parameters.CSVDef' -v 'csv'",
                "--type enum -k f -p -c -h 'A predefined way to parse the CSV input.  Other parameters may override the specifics of this definition.' -d 'Default' --class 'org.apache.commons.csv.CSVFormat' -v 'predefinedFormat' --enumlist 'org.apache.commons.csv.CSVFormat'",
                "--type boolean -k h 'header' -h 'Skip over the first line for sorting and just write it to the beginning of the output file.' -v 'header'",
                "--type byte -k c 'commentMarker' --camelcaps -h 'Sets the comment start marker of the format to the specified character. Note that the comment start character is only recognized at the start of a line.' -v 'commentMarker'",
                "--type byte -k d 'delimiter' -h 'Sets the delimiter of the format to the specified character.' -v 'delimiter'",
                "--type byte -k x 'escape' -h 'Sets the escape character of the format to the specified character.' -v 'escape'",
                "--type boolean -k e 'ignoreEmptyLines' --camelcaps -h 'Sets the empty line skipping behavior of the format to true.' -v 'ignoreEmptyLines'",
                "--type boolean -k s 'ignoreSurroundingSpaces' --camelcaps -h 'Sets the trimming behavior of the format to true.' -v 'ignoreSurroundingSpaces'",
                "--type string -k n 'nullString' --camelcaps -h 'Converts strings equal to the given nullString to null when reading records.' -v 'nullString'",
                "--type byte -k q 'quote' -h 'Sets the quoteChar of the format to the specified character.' -v 'quote'",
                "--type end -k csv",
                "--type file -k 'workDirectory' -c --camelcaps -h 'The directory where temp files will be handled.' -v 'workDirectory'",
                "--type boolean -k 'noCacheInput' --camelcaps -h 'Caching the input file into memory is faster.  This will turn off the feature.' -v 'noCacheInput'",
                "--type boolean -k 'diskWork' --camelcaps -h 'Work files are stored on disk.  The amount of memory required to hold work areas in memory is about (2 * (keySize + 24)).' -v 'diskWork'",
                "--type integer -k 'power' -h 'The depth of the funnel.  The bigger this number is, the more memory will be used.  This is computed when --max or -f is specified.' -d '16' -v 'depth' --range '2' '16'",
                "--type boolean -k 'syntaxOnly' --camelcaps -h 'Check the command - will not run' -v 'syntaxOnly'",
                "--type boolean -k 'version' -h 'Display the version of FunnelSort' -v 'version'"
            });
            final IParserInput userInput = CommandLineParser.getInstance(commandLineParser.getCommandPrefix(), args);
            commandLineParser.parse(userInput, this);
        }

        public ICmdLine getParser ()
        {
            return commandLineParser;
        }
    }

    static final private Logger logger = LoggerFactory.getLogger(FunnelContext.class);

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

        fsc = new FunnelSortContext(_args, parser);
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

    public Aggregate getAggregateByName (final String name)
    {
        if (getAggregates() != null)
            for (final Aggregate agg : getAggregates())
                if (agg.name.equalsIgnoreCase(name))
                    return agg;
        return null;
    }

    public List<Aggregate> getAggregates ()
    {
        return fsc.aggregates;
    }

    public CopyOrder getCopyOrder ()
    {
        return fsc.copyOrder;
    }

    public CSVDef getCsv ()
    {
        return fsc.csv;
    }

    public int getDepth ()
    {
        return fsc.depth;
    }

    public long getDuplicateCount ()
    {
        return duplicateCount;
    }

    public DuplicateDisposition getDuplicateDisposition ()
    {
        return fsc.duplicateDisposition;
    }

    public byte[] getEndOfRecordDelimiterIn ()
    {
        return fsc.endOfRecordDelimiterIn;
    }

    public byte[] getEndOfRecordDelimiterOut ()
    {
        return fsc.endOfRecordDelimiterOut;
    }

    public int getFixedRecordLengthIn ()
    {
        return fsc.fixedRecordLengthIn;
    }

    public int getFixedRecordLengthOut ()
    {
        return fsc.fixedRecordLengthOut;
    }

    public List<FormatPart> getFormatOutDefs ()
    {
        return fsc.formatOutDefs;
    }

    public List<KeyPart> getHeaderInDefs ()
    {
        return fsc.headerInDefs;
    }

    public List<FormatPart> getHeaderOutDefs ()
    {
        return fsc.headerOutDefs;
    }

    public List<HexDump> getHexDumps ()
    {
        return fsc.hexDumps;
    }

    public List<KeyPart> getInputColumnDefs ()
    {
        return fsc.inputColumnDefs;
    }

    public File getInputFile (final int fileNumber) throws ParseException, IOException
    {
        return fsc.inputFiles.files().get(fileNumber);
    }

    public WildFiles getInputFiles ()
    {
        return fsc.inputFiles;
    }

    public List<KeyPart> getKeys ()
    {
        return keys;
    }

    public long getMaximumNumberOfRows ()
    {
        return fsc.maximumNumberOfRows;
    }

    private List<OrderBy> getOrderBys ()
    {
        return fsc.orderBys;
    }

    public File getOutputFile ()
    {
        return fsc.outputFile;
    }

    public long getRecordCount ()
    {
        return recordCount;
    }

    public List<Equ> getStopEqu ()
    {
        return fsc.stopEqu;
    }

    public long getUnselectedCount ()
    {
        return unselectedCount;
    }

    public List<Equ> getWhereEqu ()
    {
        return fsc.whereEqu;
    }

    public File getWorkDirectory ()
    {
        return fsc.workDirectory;
    }

    public long getWriteCount ()
    {
        return writeCount;
    }

    public void inputCounters (final long p_unselectedCount, final long p_recordCount)
    {
        unselectedCount += p_unselectedCount;
        recordCount += p_recordCount;

    }

    public int inputFileCount () throws ParseException, IOException
    {
        if (getInputFiles() == null)
            return 0;
        return getInputFiles().files().size();
    }

    public int inputFileIndex ()
    {
        return inputFileIndex;
    }

    public boolean isAggregating ()
    {
        return getAggregates() != null && !getAggregates().isEmpty();
    }

    public boolean isCacheInput ()
    {
        return !fsc.noCacheInput;
    }

    public boolean isCacheWork ()
    {
        return !fsc.diskWork;
    }

    public boolean isDiskWork ()
    {
        return fsc.diskWork;
    }

    public boolean isHexDumping ()
    {
        return fsc.hexDumps != null;
    }

    public boolean isInPlaceSort ()
    {
        return fsc.inPlaceSort;
    }

    public boolean isMultisourceInput () throws ParseException, IOException
    {
        return getInputFiles() != null && getInputFiles().files().size() > 1;
    }

    public boolean isNoCacheInput ()
    {
        return fsc.noCacheInput;
    }

    public boolean isSyntaxOnly ()
    {
        return fsc.syntaxOnly;
    }

    public boolean isSysin ()
    {
        return !(fsc.getParser().arg("--inputfilename").isParsed());
    }

    public boolean isSysout () throws ParseException, IOException
    {
        if (isMultisourceInput() && isInPlaceSort())
            return false;
        return getOutputFile() == null;
    }

    public boolean isUsageRun ()
    {
        return ((CmdLine) fsc.getParser()).isUsageRun();
    }

    public boolean isUserSpecifiedOrder ()
    {
        return getOrderBys() == null || getOrderBys().isEmpty();
    }

    public boolean isVariableLengthInput ()
    {
        return fsc.getParser().arg("--variableIn").isParsed() || !(fsc.getParser().arg("--fixedIn").isParsed());
    }

    public boolean isVariableLengthOutput ()
    {
        return fsc.getParser().arg("--variableOutput").isParsed();
    }

    public boolean isVersion ()
    {
        return fsc.version;
    }

    public void outputCounters (final long p_duplicateCount, final long p_writeCount)
    {
        duplicateCount += p_duplicateCount;
        writeCount += p_writeCount;
    }

    private void postParseAggregation () throws ParseException
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

    private void postParseAnalysis () throws ParseException, IOException
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

    private void postParseCSV ()
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

    private void postParseEolOut ()
    {
        if (getEndOfRecordDelimiterOut() == null)
            fsc.endOfRecordDelimiterOut = getEndOfRecordDelimiterIn();
    }

    private void postParseFixed () throws ParseException
    {
        if (getFixedRecordLengthOut() > 0 && isVariableLengthOutput())
            throw new ParseException("--fixedOut and --variableOutput are mutually exclusive parameters", 0);
        if (isVariableLengthOutput())
            return;
        if (getFixedRecordLengthOut() == 0)
            fsc.fixedRecordLengthOut = getFixedRecordLengthIn();

    }

    private void postParseFormatOut () throws ParseException
    {
        if (getFormatOutDefs() != null)
        {
            if (getCsv() != null)
            {
                throw new ParseException("--csv and --format are mutually exclusive parameters", 0);
            }

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
                                throw new ParseException("--formatOut must be a defined column, aggregates can only be used within --equ: "
                                    + kdef.columnName, 0);
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

    private void postParseHeaderIn () throws ParseException
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
                        logger.debug("column \"{}\" length set to {} because of format", colDef.columnName, colDef.length);
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

    private void postParseHeaderOut () throws ParseException
    {
        if (getHeaderOutDefs() != null)
        {
            if (getCsv() != null)
            {
                throw new ParseException("--csv and --headerOut are mutually exclusive parameters", 0);
            }
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
                        if (!headerHelper.exists(kdef.columnName))
                        {
                            throw new ParseException("--headerOut must be a defined headerIn: " + kdef.columnName, 0);
                        }
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

    private void postParseHexDumps () throws ParseException
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
                if (KeyType.String.name().equalsIgnoreCase(column.typeName)
                    || KeyType.Byte.name().equalsIgnoreCase(column.typeName))
                {
                    // ok
                } else
                    throw new ParseException("HexDump can only be on String or Byte columns: " + hexDump.columnName, 0);
            }
        }
    }

    private void postParseInputColumns () throws ParseException
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
                        logger.debug("column \"{}\" length set to {} because of format", colDef.columnName, colDef.length);
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

    private void postParseInputFile () throws ParseException, IOException
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

    private void postParseOrderBy () throws ParseException
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
                if (kdef.isCsv())
                {
                    throw new ParseException("unexpected CSV key (--field) on a non-CSV file", 0);
                }
            }
        /*
         * Check for non-cvs keys on a cvs file
         */
        if (keys != null && getCsv() != null)
            for (final KeyPart kdef : keys)
            {
                if (!kdef.isCsv())
                {
                    throw new ParseException("only CSV keys (--field) allowed on a CSV file", 0);
                }
            }
        /*
         * Check for duplicate csv keys
         */
        if (keys != null && getCsv() != null)
            for (final KeyPart k1 : keys)
            {
                for (final KeyPart k2 : keys)
                {
                    if (k1 != k2 && k1.csvFieldNumber == k2.csvFieldNumber)
                    {
                        throw new ParseException("sorting on the same field (--field "
                            + k2.csvFieldNumber
                            + ") is not allowed", 0);
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
                    throw new ParseException(e.getMessage(), 0);
                }
            }
    }

    private void postParseOutputFile () throws ParseException, IOException
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

    public void reset () throws IOException, ParseException
    {
        if (provider != null)
            provider.reset();
        if (publisher != null)
            publisher.reset();
    }

    public void setDepth (final int optimalFunnelDepth)
    {
        fsc.depth = optimalFunnelDepth;
    }

    public void setInputFiles (final WildFiles wildFiles)
    {
        fsc.inputFiles = wildFiles;
    }

    public void setOutputFile (final File outputFile)
    {
        fsc.outputFile = outputFile;
    }

    /**
     *
     */
    void showParameters () throws IOException, ParseException
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
                showParametersLog(false, "col \"{}\" {} offset {} length {} {}", col.columnName, col.typeName, col.offset, col.length, (col.parseFormat == null
                        ? ""
                        : " format " + col.parseFormat));
            else
                showParametersLog(false, "col {} {} csvField {} {}", col.columnName, col.typeName, col.csvFieldNumber, (col.parseFormat == null
                        ? ""
                        : " format " + col.parseFormat));
        }

        for (final String colName : headerHelper.getNames())
        {
            final KeyPart col = headerHelper.get(colName);
            showParametersLog(false, "headerIn \"{}\" {} offset {} length {} {}", col.columnName, col.typeName, col.offset, col.length, (col.parseFormat == null
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

    private void showParametersLog (final boolean forceInfo, final String message, final Object... parms)
    {
        if (forceInfo || isSyntaxOnly())
            logger.info(message, parms);
        else
            logger.debug(message, parms);
    }

    public boolean startNextInput () throws ParseException, IOException
    {
        /*
         * Has the last input file been read? Then return false.
         */
        if (inputFileIndex() >= (inputFileCount() - 1))
            return false;
        inputFileIndex++;
        return true;
    }

    public boolean stopIsTrue () throws Exception
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

    public boolean whereIsTrue () throws Exception
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