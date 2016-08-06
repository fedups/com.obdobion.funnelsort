package com.obdobion.funnel.columns;

import java.io.ByteArrayOutputStream;
import java.util.Formatter;

import com.obdobion.algebrain.Equ;
import com.obdobion.algebrain.token.TokVariable;
import com.obdobion.argument.annotation.Arg;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FormatPart
{
    @Arg(positional = true, allowCamelCaps = true, help = "A previously defined column name.")
    public String     columnName;

    @Arg(shortName = 't',
            longName = "type",
            positional = true,
            caseSensitive = true,
            help = "The data type to be written.  Defaults to the columnIn data type.")
    public KeyType    typeName;

    @Arg(shortName = 'e',
            allowMetaphone = true,
            help = "Used instead of a column name, this will be evaluated with the result written to the output.")
    public Equ        equation;

    @Arg(shortName = 'd',
            caseSensitive = true,
            help = "The format for converting the contents of the data to be written. Use Java Formatter rules for making the format.  The format must match the type of the data.")
    public String     format;

    @Arg(shortName = 'o',
            defaultValues = { "-1" },
            range = { "0" },
            help = "The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.")
    public int        offset;

    @Arg(shortName = 'l', defaultValues = { "255" }, range = { "1", "255" }, help = "The length of the key in bytes.")
    public int        length;

    @Arg(shortName = 's',
            defaultValues = { "255" },
            range = { "1", "255" },
            help = "The number of characters this field will use on output.")
    public int        size;

    @Arg(shortName = 'f', defaultValues = { " " }, help = "The trailing filler character to use for a short field.")
    public byte       filler;

    public FormatPart nextPart;
    KeyPart           column;

    public FormatPart()
    {
        super();
        columnName = null;
        filler = ' ';
    }

    public void add(final FormatPart anotherFormatter)
    {
        if (nextPart == null)
            nextPart = anotherFormatter;
        else
            nextPart.add(anotherFormatter);
    }

    public void defineFrom(final KeyPart colDef)
    {
        column = colDef;
    }

    public void originalData(
            final KeyContext keyContext,
            final FunnelContext funnelContext,
            final int originalSize,
            final ByteArrayOutputStream outputBytes) throws Exception
    {
        if (column != null)
            writeToOutput(outputBytes, keyContext.rawRecordBytes[0], column.offset, column.length, originalSize);
        else if (equation != null)
        {
            final Object result = equation.evaluate();
            if (result instanceof TokVariable)
                throw new Exception("invalid equation result for --format(" + equation.toString() + ")");

            if (typeName == null || KeyType.String == typeName)
                if (result instanceof String)
                {
                    final String sResult = (String) result;
                    writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                } else if (format == null)
                {
                    final String sResult = result.toString();
                    writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                } else
                    try (Formatter formatter = new Formatter())
                    {
                        final String sResult = formatter.format(format, result).out().toString();
                        writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                    }
        } else if (funnelContext.headerHelper.exists(columnName))
        {
            final KeyPart headerCol = funnelContext.headerHelper.get(columnName);
            final byte[] result = funnelContext.headerHelper.getContents(headerCol);
            writeToOutput(outputBytes, result, offset, result.length, result.length);

        } else
            /*
             * filler only
             */
            writeToOutput(outputBytes, null, 0, 0, 0);
        if (nextPart != null)
            nextPart.originalData(keyContext, funnelContext, originalSize, outputBytes);
    }

    private void writeToOutput(
            final ByteArrayOutputStream outputBytes,
            final byte[] rawBytes,
            final int columnOffset,
            final int columnLength,
            final int originalSize)
    {
        final int offsetForOutput = columnOffset + offset;

        int dataLength = columnLength;
        if (originalSize < offsetForOutput + dataLength)
            dataLength = originalSize - offsetForOutput;
        if (length < dataLength)
            dataLength = length;
        if (rawBytes != null)
            outputBytes.write(rawBytes, offsetForOutput, dataLength);

        int lengthWithFiller = columnLength;
        if (length != 255) // 255 means not specified
            lengthWithFiller = length;
        if (size != 255) // 255 means not specified
            lengthWithFiller = size;
        if (lengthWithFiller == 255)
            lengthWithFiller = dataLength;
        if (lengthWithFiller > dataLength)
            for (int x = 0; x < lengthWithFiller - dataLength; x++)
                outputBytes.write(filler);
    }
}