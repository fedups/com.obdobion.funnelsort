Funnel is a sort / copy / merge utility.

    [positional <wildfiles>]
        The input file or files to be processed.  Wild cards are allowed in the
        filename only and the path (** indicates multiple path segments).
        Sysin is assumed if this parameter is not provided.
        Although no value is required, if you specify one you must specify at
        least 1.
        Upper vs lower case matters.

    [-o --outputfilename <file>]
        The output file to be written.  Sysout is assumed if this parameter is
        not provided.  The same name as the input file is allowed.
        Upper vs lower case matters.

    [-f --fixed <integer>]
        The record length in a fixed record length file.
        The value must be from 1 to 4096 inclusive

    [-r --replace]
        Overwrite the input file with the results.  --outputFile is not
        required with this parameter.  --outputFile is assumed.

    [--columnsIn ()]
        Column definitions defining the input file layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        [-n --name <string>]
            A name for this column / key so that it can be referenced.

        positional <enum>
            The data type of the key in the file.
            Upper vs lower case matters.
            Possible choices are: String Integer Float BInteger BFloat Date

        [-f --field <integer>]
            If this is a CSV file then use this instead of offset and length.
            The value must be at least 0.

        [-o --offset <integer>]
            The zero relative offset from the beginning of a row.  This will be
            computed, if not specified, to be the location of the previous
            column plus the length of the previous column.  Most often this
            parameter is not needed.
            The value must be at least 0.
            If unspecified, the default will be 0.

        [-l --length <integer>]
            The length of the key in bytes.
            The value must be from 1 to 255 inclusive
            If unspecified, the default will be 255.

        [-d --format <string>]
            The parsing format for converting the contents of the key in the
            file to an internal representation. Use Java SimpleDateFormat rules
            for making the format.
            Upper vs lower case matters.

    [--formatOut ()]
        Column references defining the output file layout.

        Although no value is required, if you specify one you must specify at
        least 1.
        positional <string>
            A previously defined column name.

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
            If unspecified, the default will be 0.

        [-f --filler <byte>]
            The trailing filler character to use for a short field.

    [-w --where <string>]
        Rows that evaluate to TRUE are selected for Output.  See "Algebrain"
        for details.  Columns are used as variables in this Algebrain equation.
        Although no value is required, if you specify one you must specify at
        least 1.

    [-s --stopWhen <string>]
        The sort will stop reading input when this equation returns TRUE.  See
        "Algebrain" for details.  Columns are used as variables in this
        Algebrain equation.
        Although no value is required, if you specify one you must specify at
        least 1.

    [--eol <byte>]
        The byte(s) that end each line in a variable length record file.
        Although no value is required, if you specify one you must specify at
        least 1.
        If unspecified, the default will be cr(13) lf(10).

    [--eolOut <byte>]
        The byte(s) that end each line in a variable length record file.  This
        will be used to write the output file.  If this is not specified then
        the --eol value will be used.
        Although no value is required, if you specify one you must specify at
        least 1.

    [-d --duplicate <enum>]
        Special handling of duplicate keyed rows.
        Upper vs lower case matters.
        Possible choices are: FirstOnly LastOnly Reverse Original
        If unspecified, the default will be Original.

    [-c --copy <enum>]
        Defines the process that will take place on the input.
        Upper vs lower case matters.
        Possible choices are: Reverse Original ByKey
        If unspecified, the default will be ByKey.

    [--maxrows <long>]
        Used for variable length input, estimate the number of rows.  Too low
        could cause problems.
        The value must be at least 2.
        If unspecified, the default will be 9223372036854775807.

    [--orderby ()]
        The sort keys defined from columns.

        Although no value is required, if you specify one you must specify at
        least 1.
        positional <string>
            A previously defined column name.

        [positional <enum>]
            The direction of the sort for this key. AASC and ADESC are absolute
            values of the key - the case of letters would not matter and the
            sign of numbers would not matter.
            Upper vs lower case matters.
            Possible choices are: ASC DESC AASC ADESC
            If unspecified, the default will be ASC.

    [--csv ()]
        The definition of the CSV file being read as input.  Using this
        indicates that the input is in fact a CSV file and the columns
        parameter must use the --field arguments.

        [positional <enum>]
            A predefined way to parse the CSV input.  Other parameters may
            override the specifics of this definition.
            Upper vs lower case matters.
            Possible choices are: Default Excel MySQL RFC4180 TDF
            If unspecified, the default will be Default.

        [-h --header]
            Skip over the first line for sorting and just write it to the
            beginning of the output file.

        [-c --commentMarker <byte>]
            Sets the comment start marker of the format to the specified
            character. Note that the comment start character is only recognized
            at the start of a line.

        [-d --delimiter <byte>]
            Sets the delimiter of the format to the specified character.

        [-x --escape <byte>]
            Sets the escape character of the format to the specified character.

        [-e --ignoreEmptyLines]
            Sets the empty line skipping behavior of the format to true.

        [-s --ignoreSurroundingSpaces]
            Sets the trimming behavior of the format to true.

        [-n --nullString <string>]
            Converts strings equal to the given nullString to null when reading
            records.

        [-q --quote <byte>]
            Sets the quoteChar of the format to the specified character.

    [--variableoutput]
        Use this to cause a fixed input to be written as variable.

    [--workDirectory <file>]
        The directory where temp files will be handled.
        Upper vs lower case matters.
        If unspecified, the default will be \tmp.

    [--cacheInput]
        Read the input file into memory.  This saves reading it again on
        multipass sorts.  The amount of memory required to hold the input file
        in core is equal to the size of the file.

    [--diskWork]
        Work files are stored on disk.  The amount of memory required to hold
        work areas in memory is about (2 * (keySize + 24)).

    [--power <integer>]
        The depth of the funnel.  The bigger this number is, the more memory
        will be used.  This is computed when --max or -f is specified.
        The value must be from 2 to 16 inclusive
        If unspecified, the default will be 16.

    [--version]
        Display the version of Funnel
