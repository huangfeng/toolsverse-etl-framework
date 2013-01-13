/*
 * BaseDataSetConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.ext.BaseExtension;
import com.toolsverse.util.FileUtils;

/**
 * The base class for all DataSetConnectors.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseDataSetConnector extends BaseExtension
{
    
    /**
     * Adds value to the current field of the current record.  
     *
     * @param colValue the value
     * @param record the record
     * @param dataSet the data set
     */
    public void addValue(Object colValue, DataSetRecord record, DataSet dataSet)
    {
        record.add(colValue);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getType()
     */
    public String getType()
    {
        return "Connector";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#testConnection(com.toolsverse
     * .etl.common.ConnectionParams)
     */
    public String testConnection(ConnectionParams connectionParams)
        throws Exception
    {
        if (!(connectionParams instanceof Alias)
                || connectionParams.isDbConnection())
        {
            return ConnectorResource.UNKNOWN_CONNECTION_TYPE.getValue();
        }
        
        Alias alias = (Alias)connectionParams;
        
        Boolean hasWildcard = FileUtils.hasWildCard(alias.getUrl());
        
        if (Boolean.FALSE.equals(hasWildcard))
        {
            if (FileUtils.fileExists(alias.getUrl()))
                return "";
            else
                return ConnectorResource.FILE_DOES_NOT_EXIST.getValue();
        }
        else if (Boolean.TRUE.equals(hasWildcard))
        {
            return ConnectorResource.FILE_NAME_HAS_WILDCARD.getValue();
        }
        
        return "";
    }
    
}
