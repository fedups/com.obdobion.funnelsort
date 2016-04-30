package com.obdobion.funnel.columns;

import java.io.ByteArrayOutputStream;

import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
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

    public void defineFrom (KeyPart colDef)
    {
        column = colDef;
    }

    public void originalData (final KeyContext context, SourceProxyRecord proxyRecord, ByteArrayOutputStream outputBytes)
    {
        final byte[] rawBytes = context.rawRecordBytes[0];

        int offsetForOutput = column.offset + offset;

        int dataLength = column.length;
        if (proxyRecord.originalSize < offsetForOutput + dataLength)
            dataLength = proxyRecord.originalSize - offsetForOutput;
        if (length < dataLength)
            dataLength = length;
        outputBytes.write(rawBytes, offsetForOutput, dataLength);

        int lengthWithFiller = column.length;
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

        if (nextPart != null)
            nextPart.originalData(context, proxyRecord, outputBytes);
    }
}