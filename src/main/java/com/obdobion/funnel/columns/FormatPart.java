package com.obdobion.funnel.columns;

import java.io.ByteArrayOutputStream;
import java.util.Formatter;

import com.obdobion.algebrain.Equ;
import com.obdobion.algebrain.TokVariable;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
public class FormatPart
{
    public int        length;
    public int        offset;
    public int        size;
    public FormatPart nextPart;
    public String     columnName;
    public byte       filler;
    public Equ        equation;
    public KeyType    typeName;
    public String     format;

    KeyPart           column;

    public FormatPart()
    {
        super();
        columnName = null;
        filler = ' ';
    }

    public void add (final FormatPart anotherFormatter)
    {
        if (nextPart == null)
            nextPart = anotherFormatter;
        else
            nextPart.add(anotherFormatter);
    }

    public void defineFrom (final KeyPart colDef)
    {
        column = colDef;
    }

    public void originalData (
        final KeyContext context,
        final SourceProxyRecord proxyRecord,
        final ByteArrayOutputStream outputBytes) throws Exception
    {
        if (column != null)
        {
            writeToOutput(outputBytes, context.rawRecordBytes[0], column.offset, column.length, proxyRecord.originalSize);
        } else
        {
            if (equation != null)
            {
                final Object result = equation.evaluate();
                if (result instanceof TokVariable)
                    throw new Exception("invalid equation result for --format(" + equation.toString() + ")");

                if (typeName == null || KeyType.String == typeName)
                {
                    if (result instanceof String)
                    {
                        final String sResult = (String) result;
                        writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                    } else if (format == null)
                    {
                        final String sResult = result.toString();
                        writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                    } else
                    {
                        try (Formatter formatter = new Formatter())
                        {
                            final String sResult = formatter.format(format, result).out().toString();
                            writeToOutput(outputBytes, sResult.getBytes(), offset, sResult.length(), sResult.length());
                        }
                    }
                }
            } else
            {
                /*
                 * filler only
                 */
                writeToOutput(outputBytes, null, 0, 0, 0);
            }
        }
        if (nextPart != null)
            nextPart.originalData(context, proxyRecord, outputBytes);
    }

    private void writeToOutput (
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
        {
            for (int x = 0; x < lengthWithFiller - dataLength; x++)
                outputBytes.write(filler);
        }
    }
}