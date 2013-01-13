/*
 * MergeHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.sql.Connection;

/**
 * Defines common interface for the class which will be used when default etl "insert into" action is failed and etl scenario is configured to try update instead.
 *
 * @see com.toolsverse.etl.common.OnException
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface MergeHandler
{
    
    /**
     * Executed when default etl "insert into" action is failed and etl scenario is configured to try update instead. Usually translates insert sql
     * statement into update and executes it.   
     *
     * @param onException the OnException object
     * @param conn the connection
     * @param sql the sql (usually insert statement)
     * @param keyField the key field
     * @param row the row
     * @throws Exception in case of any error
     */
    void onMerge(OnException onException, Connection conn, String sql,
            String keyField, long row)
        throws Exception;
}
