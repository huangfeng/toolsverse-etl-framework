/*
 * AddFieldValueCallback.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.driver.Driver;

/**
 * Handler for the "add field value" event. When DataSetConnector extracts data it sets values to the fields as it goes. AddFieldValueCallback#onAddFieldValue 
 * is called right after value is set for the field. 
 *
 * @see com.toolsverse.etl.connector.DataSetConnector
 * @see com.toolsverse.etl.common.FieldDef
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface AddFieldValueCallback
{
    
    /**
     * Called when value is set for the field.
     *
     * @param dataSet the data set
     * @param driver the driver
     * @param record the record
     * @param fieldDef the field
     * @throws Exception in case of any error
     */
    void onAddFieldValue(DataSet dataSet, Driver driver, DataSetRecord record,
            FieldDef fieldDef)
        throws Exception;
}
