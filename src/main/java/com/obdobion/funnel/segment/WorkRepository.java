package com.obdobion.funnel.segment;

import java.io.IOException;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * WorkRepository interface.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface WorkRepository
{
    /**
     * <p>
     * close.
     * </p>
     *
     * @throws java.io.IOException if any.
     */
    public abstract void close() throws IOException;

    /**
     * <p>
     * delete.
     * </p>
     *
     * @throws java.io.IOException if any.
     */
    public abstract void delete() throws IOException;

    /**
     * <p>
     * getContext.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     */
    public FunnelContext getContext();

    /**
     * <p>
     * open.
     * </p>
     *
     * @throws java.io.IOException if any.
     */
    public abstract void open() throws IOException;

    /**
     * <p>
     * outputPosition.
     * </p>
     *
     * @return a long.
     */
    public abstract long outputPosition();

    /**
     * <p>
     * read.
     * </p>
     *
     * @param position a long.
     * @param rec a {@link com.obdobion.funnel.segment.SourceProxyRecord}
     *            object.
     * @return a long.
     * @throws java.io.IOException if any.
     */
    public abstract long read(final long position, final SourceProxyRecord rec) throws IOException;

    /**
     * <p>
     * write.
     * </p>
     *
     * @param rec a {@link com.obdobion.funnel.segment.SourceProxyRecord}
     *            object.
     * @return a long.
     * @throws java.io.IOException if any.
     */
    public abstract long write(SourceProxyRecord rec) throws IOException;
}
