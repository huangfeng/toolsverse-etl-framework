/*
 * AfterCallback.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.driver.Driver;

/**
 * When DataSetConnector finishes populating data set it executes AfterCallback#onAfter.    
 *
 * @see com.toolsverse.etl.connector.DataSetConnector
 * @see com.toolsverse.etl.common.FieldDef
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface AfterCallback
{
    
    /**
     * Executed when DataSetConnector finishes populating data set.
     *
     * @param dataSet the data set
     * @param driver the driver
     * @throws Exception in case of any error
     */
    void onAfter(DataSet dataSet, Driver driver)
        throws Exception;
}
