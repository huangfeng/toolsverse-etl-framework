/*
 * GenericFileDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.sql.Connection;
import java.sql.Types;
import java.util.Date;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * Default ETL driver for the file based data sources, such as text files, xml files, Excel etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class GenericFileDriver extends SimpleDriver
{
    
    /** The jdbc driver class name. */
    private static final String JDBC_DRIVER_CLASS = "";
    
    /** The url. */
    private static final String JDBC_URL = "";
    
    /** The metadata class name. */
    private static final String METADATA_CLASS = "";
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.SimpleDriver#convertStringForStorage(java.lang
     * .String)
     */
    @Override
    public String convertStringForStorage(String value)
    {
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.SimpleDriver#convertValueForStorage(java.lang
     * .Object, int, boolean)
     */
    @Override
    public String convertValueForStorage(Object fieldValue, int fieldType,
            boolean isFromTable)
    {
        if (fieldValue == null)
            return "";
        
        String value = fieldValue.toString();
        
        if (SqlUtils.isChar(fieldType) || SqlUtils.isClob(fieldType))
        {
            return value;
        }
        
        switch (fieldType)
        {
            case Types.DATE:
                if (fieldValue instanceof Date)
                    return Utils.date2Str((Date)fieldValue,
                            DataSet.DATA_SET_DATE_ONLY_FORMAT);
                else
                    return value;
            case Types.TIME:
                if (fieldValue instanceof Date)
                    return Utils.date2Str((Date)fieldValue,
                            DataSet.DATA_SET_TIME_FORMAT);
                else
                    return value;
            case Types.TIMESTAMP:
                if (fieldValue instanceof Date)
                    return Utils.date2Str((Date)fieldValue,
                            DataSet.DATA_SET_DATE_TIME_FORMAT);
                else
                    return value;
            default:
                return value;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getCmdForExternalTool(com.toolsverse
     * .util.ObjectStorage, com.toolsverse.etl.common.Alias, java.lang.String)
     */
    public String getCmdForExternalTool(ObjectStorage storage, Alias alias,
            String sqlFile)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getExplainPlan(com.toolsverse.util.
     * ObjectStorage, java.sql.Connection, com.toolsverse.etl.common.Alias,
     * java.lang.String)
     */
    public Object getExplainPlan(ObjectStorage storage, Connection connection,
            Alias alias, String sql)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getExternalToolName()
     */
    public String getExternalToolName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getIconPath()
     */
    public String getIconPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getJdbcDriverClassName()
     */
    public String getJdbcDriverClassName()
    {
        return JDBC_DRIVER_CLASS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
     */
    public String getLicensePropertyName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMetadataClassName()
     */
    public String getMetadataClassName()
    {
        return METADATA_CLASS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getName()
     */
    public String getName()
    {
        return "Generic File";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getSqlForExternalTool(com.toolsverse
     * .etl.common.Alias, java.lang.String, com.toolsverse.etl.parser.SqlParser)
     */
    public String getSqlForExternalTool(Alias alias, String sql,
            SqlParser parser)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getUrlPattern()
     */
    public String getUrlPattern()
    {
        return JDBC_URL;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVendor()
     */
    public String getVendor()
    {
        return SystemConfig.DEFAULT_VENDOR;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVersion()
     */
    public String getVersion()
    {
        return "3.1";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getXmlConfigFileName()
     */
    public String getXmlConfigFileName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.AbstractDriver#supportsParallelExtract()
     */
    @Override
    public boolean supportsParallelExtract()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.AbstractDriver#supportsParallelLoad()
     */
    @Override
    public boolean supportsParallelLoad()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsScripts()
     */
    public boolean supportsScripts()
    {
        return false;
    }
    
}
