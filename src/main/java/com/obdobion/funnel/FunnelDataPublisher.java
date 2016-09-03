package com.obdobion.funnel;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * <p>
 * FunnelDataPublisher interface.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface FunnelDataPublisher
{
    /**
     * <p>
     * close.
     * </p>
     *
     * @throws java.lang.Exception if any.
     */
    public void close() throws Exception;

    /**
     * <p>
     * getDuplicateCount.
     * </p>
     *
     * @return a long.
     */
    public long getDuplicateCount();

    /**
     * <p>
     * getWriteCount.
     * </p>
     *
     * @return a long.
     */
    public long getWriteCount();

    /**
     * <p>
     * openInput.
     * </p>
     *
     * @throws java.lang.Exception if any.
     */
    public void openInput() throws Exception;

    /**
     * <p>
     * publish.
     * </p>
     *
     * @param item a {@link com.obdobion.funnel.segment.SourceProxyRecord}
     *            object.
     * @param phase a long.
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public boolean publish(SourceProxyRecord item, long phase) throws Exception;

    /**
     * <p>
     * reset.
     * </p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void reset() throws IOException, ParseException;
}
