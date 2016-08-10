package com.obdobion.funnel.orderby;

/**
 * <p>KeyContext class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class KeyContext
{
    public long     recordNumber;
    /*
     * Only the first occurrence is used for non-csv files. All occurrences are
     * for the fields in the csv row.
     */
    public byte[][] rawRecordBytes;
    public byte[]   key;
    public int      keyLength;
}
