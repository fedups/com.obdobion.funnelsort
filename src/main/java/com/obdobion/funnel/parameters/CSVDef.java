package com.obdobion.funnel.parameters;

import org.apache.commons.csv.CSVFormat;

import com.obdobion.argument.annotation.Arg;

/**
 * @author Chris DeGreef
 *
 */
public class CSVDef
{
    @Arg(shortName = 'f',
            longName = "",
            positional = true,
            caseSensitive = true,
            defaultValues = "Default",
            help = "A predefined way to parse the CSV input.  Other parameters may override the specifics of this definition.")
    // --enumlist 'org.apache.commons.csv.CSVFormat'
    public CSVFormat.Predefined predefinedFormat;

    @Arg(shortName = 'h',
            help = "Skip over the first line for sorting and just write it to the beginning of the output file.")
    public boolean              header;

    @Arg(shortName = 'c',
            allowCamelCaps = true,
            help = "Sets the comment start marker of the format to the specified character. Note that the comment start character is only recognized at the start of a line.")
    public byte                 commentMarker;

    @Arg(shortName = 'd', help = "Sets the delimiter of the format to the specified character.")
    public byte                 delimiter;

    @Arg(shortName = 'x', help = "Sets the escape character of the format to the specified character.")
    public byte                 escape;

    @Arg(shortName = 'e', allowCamelCaps = true, help = "Sets the empty line skipping behavior of the format to true.")
    public boolean              ignoreEmptyLines;

    @Arg(shortName = 's', allowCamelCaps = true, help = "Sets the trimming behavior of the format to true.")
    public boolean              ignoreSurroundingSpaces;

    @Arg(shortName = 'n',
            allowCamelCaps = true,
            help = "Converts strings equal to the given nullString to null when reading records.")
    public String               nullString;

    @Arg(shortName = 'q', help = "Sets the quoteChar of the format to the specified character.")
    public byte                 quote;

    public byte[]               headerContents;
    public CSVFormat            format;
}