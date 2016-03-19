package com.obdobion.funnel.columns;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.obdobion.algebrain.Equ;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;

/**
 * @author Chris DeGreef
 * 
 */
public class ColumnHelper
{
    final private static Logger logger       = Logger.getLogger(ColumnHelper.class);

    public static final int     MAX_COLUMN_SIZE = 255;
    final KeyContext            context;
    final int                   maxKeyBytes;
    List<KeyPart>               columns;

    public ColumnHelper()
    {
        this(MAX_COLUMN_SIZE);
    }

    public ColumnHelper(final int maxsize)
    {
        logger.debug("maximum column length is " + MAX_COLUMN_SIZE);

        maxKeyBytes = maxsize;
        context = new KeyContext();
        columns = new ArrayList<>();
    }

    public void add (final KeyPart formatter) throws ParseException
    {
        if (exists(formatter.columnName))
            throw new ParseException("Column already defined: " + formatter.columnName, 0);
        final KeyPart myCopy = formatter.newCopy();
        columns.add(myCopy);
    }

    public boolean exists (final String name)
    {
        for (final KeyPart col : columns)
        {
            if (col.columnName.equalsIgnoreCase(name))
                return true;
        }
        return false;
    }

    public KeyPart get (final String name)
    {
        for (final KeyPart col : columns)
        {
            if (col.columnName.equalsIgnoreCase(name))
                return col;
        }
        return null;
    }

    /**
     * It is likely that the provided data is a reusable buffer of bytes. So we
     * can't just store these bytes for later use.
     */
    public KeyContext extract (final byte[] data, final long recordNumber, final int dataLength)
            throws Exception
    {
        /*
         * The extra byte is for a 0x00 character to be placed at the end of
         * String keys. This is important in order to handle keys where the user
         * specified the maximum length for a String key. Or took the default
         * sort, which is the maximum key.
         */
        context.key = new byte[maxKeyBytes + 1];
        context.keyLength = 0;
        context.rawRecordBytes = new byte[1][];
        context.rawRecordBytes[0] = data;
        context.recordNumber = recordNumber;
        for (final KeyPart col : columns)
        {
            Equ.getInstance().getSupport().assignVariable(col.columnName, col.parseObjectFromRawData(context));
        }
        Equ.getInstance().getSupport().assignVariable("recordnumber", new Long(recordNumber + 1));
        Equ.getInstance().getSupport().assignVariable("recordsize", new Integer(dataLength));

        context.rawRecordBytes = null;
        return context;
    }

    /**
     * Call this method for csv files that break each row up into fields (byte
     * arrays). [][].
     * 
     * @param data
     * @param recordNumber
     * @return
     * @throws Exception
     */
    public KeyContext extract (final byte[][] data, final long recordNumber) throws Exception
    {
        /*
         * The extra byte is for a 0x00 character to be placed at the end of
         * String keys. This is important in order to handle keys where the user
         * specified the maximum length for a String key. Or took the default
         * sort, which is the maximum key.
         */
        // context.key = new byte[maxKeyBytes + 1];
        // context.keyLength = 0;
        // context.rawRecordBytes = data;
        // context.recordNumber = recordNumber;
        //
        // formatter.format(context);
        //
        // context.rawRecordBytes = null;
        return context;
    }

    public KeyContext extract (final String data, final long recordNumber) throws Exception
    {
        // context.key = new byte[maxKeyBytes];
        // context.keyLength = 0;
        // context.rawRecordBytes = new byte[1][];
        // context.rawRecordBytes[0] = data.getBytes();
        // context.recordNumber = recordNumber;
        //
        // formatter.format(context);
        //
        // context.rawRecordBytes = null;
        return context;
    }

    public boolean whereIsTrue () throws Exception
    {
        final Object result = Equ.getInstance().evaluate();
        if (result == null)
            return true;
        if (!(result instanceof Boolean))
            throw new Exception("--where clause must evaluate to true or false");
        return ((Boolean) result).booleanValue();
    }

    public List<String> getNames ()
    {
        final List<String> allNames = new ArrayList<>();
        for (final KeyPart col : columns)
        {
            allNames.add(col.columnName);
        }
        return allNames;
    }
}