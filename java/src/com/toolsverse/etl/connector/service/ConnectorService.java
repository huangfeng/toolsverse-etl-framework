/*
 * ConnectorService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.service;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.connector.ConnectorResult;
import com.toolsverse.service.Service;

/**
 * The particular implementation of this interface should be called to persist and populate data sets.
 *
 * @see com.toolsverse.etl.common.DataSet
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ConnectorService extends Service
{
    
    /**
     * Persists data set.
     *
     * @param request the request
     * @param dataSet the data set
     * @return the connector result
     * @throws Exception in case of any error
     */
    ConnectorResult persistDataSet(ConnectorRequest request, DataSet dataSet)
        throws Exception;
    
    /**
     * Populates data set.
     *
     * @param request the request
     * @return the data set
     * @throws Exception in case of any error
     */
    DataSet populateDataSet(ConnectorRequest request)
        throws Exception;
    
}
