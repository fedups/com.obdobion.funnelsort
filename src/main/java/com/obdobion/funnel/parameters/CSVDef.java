package com.obdobion.funnel.parameters;

import org.apache.commons.csv.CSVFormat;

/**
 * @author Chris DeGreef
 * 
 */
public class CSVDef
{
    public CSVFormat.Predefined predefinedFormat;

    public boolean              header;

    public byte                 commentMarker;
    public byte                 delimiter;
    public byte                 escape;
    public boolean              ignoreEmptyLines;
    public boolean              ignoreSurroundingSpaces;
    public String               nullString;
    public byte                 quote;

    public byte[]               headerContents;

    public CSVFormat            format;
}