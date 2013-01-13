/*
 * AddRecordCallback.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.driver.Driver;

/**
 * Handler for the "add record" event. When DataSetConnector extracts data it adds records to the data set as it goes. AddRecordCallback#onAddRecord
 * is called right after record is added to the data set. 
 *
 * @see com.toolsverse.etl.connector.DataSetConnector
 * @see com.toolsverse.etl.common.FieldDef
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface AddRecordCallback
{
    
    /**
     * Called when record is added to the data set.
     *
     * @param dataSet the data set
     * @param driver the driver
     * @param record the record
     * @param index the index
     * @throws Exception in case of any error
     */
    void onAddRecord(DataSet dataSet, Driver driver, DataSetRecord record,
            int index)
        throws Exception;
}
