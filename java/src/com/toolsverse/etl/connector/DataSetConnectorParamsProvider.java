/*
 * DataSetConnectorParamsProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

/**
 * Creates {@link DataSetConnectorParams} objects.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface DataSetConnectorParamsProvider
{
    
    /**
     * Creates {@link DataSetConnectorParams} object.
     *
     * @return the DataSetConnectorParams object
     */
    DataSetConnectorParams getDataSetConnectorParams();
}
