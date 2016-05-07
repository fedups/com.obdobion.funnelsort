package com.obdobion.funnel.orderby;

import java.io.ByteArrayOutputStream;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 * 
 */
abstract public class KeyPart
{
    public int          csvFieldNumber;
    public int          offset;
    public int          length;
    public KeyDirection direction;
    public String       parseFormat;
    public KeyPart      nextPart;
    public String       typeName;
    public String       columnName;

    public KeyPart()
    {
        super();
        csvFieldNumber = -1;
        typeName = null;
        columnName = null;
        offset = -1;
    }

    public void add (final KeyPart anotherFormatter)
    {
        if (nextPart == null)
            nextPart = anotherFormatter;
        else
            nextPart.add(anotherFormatter);
    }

    abstract public void pack (KeyContext context) throws Exception;

    abstract public Object parseObjectFromRawData (final KeyContext context) throws Exception;

    public boolean isCsv ()
    {
        return csvFieldNumber >= 0;
    }

    byte[] rawBytes (final KeyContext context)
    {
        if (csvFieldNumber >= 0)
            return context.rawRecordBytes[csvFieldNumber];
        return context.rawRecordBytes[0];
    }

    /**
     * Copy everything exception the key specific things like direction and
     * nextPart. Even if this is the key that caused the column to be defined we
     * can still copy the values since they would be the same.
     * 
     * @param colDef
     */
    public void defineFrom (KeyPart colDef)
    {
        csvFieldNumber = colDef.csvFieldNumber;
        offset = colDef.offset;
        length = colDef.length;
        parseFormat = colDef.parseFormat;
        typeName = colDef.typeName;
        columnName = colDef.columnName;
    }

    public KeyPart newCopy ()
    {
        KeyPart myCopy;
        try
        {
            myCopy = getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e)
        {
            e.printStackTrace();
            return null;
        }
        myCopy.csvFieldNumber = csvFieldNumber;
        myCopy.offset = offset;
        myCopy.length = length;
        myCopy.parseFormat = parseFormat;
        myCopy.typeName = typeName;
        myCopy.direction = direction;
        myCopy.columnName = columnName;
        return myCopy;
    }

    public void originalData (final KeyContext context,  SourceProxyRecord proxyRecord, ByteArrayOutputStream outputBytes)
    {
        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (proxyRecord.originalSize < offset + length)
            lengthThisTime = proxyRecord.originalSize - offset;

        outputBytes.write(rawBytes, offset, lengthThisTime);
        if (context.keyLength > lengthThisTime)
        {
            for (int x = 0; x < context.keyLength - lengthThisTime; x++)
                outputBytes.write(' ');
        }

        if (nextPart != null)
            nextPart.originalData(context, proxyRecord, outputBytes);
    }
}