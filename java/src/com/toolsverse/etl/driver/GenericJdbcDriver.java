/*
 * GenericJdbcDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.sql.Connection;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.util.ObjectStorage;

/**
 * Default ETL driver for the jdbc compliant databases which does not support callable statements.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class GenericJdbcDriver extends SimpleDriver
{
    
    /** The jdbc driver class name. */
    private static final String JDBC_DRIVER_CLASS = "";
    
    /** The url. */
    private static final String JDBC_URL = "";
    
    /** The metadata driver class name. */
    private static final String METADATA_CLASS = "com.toolsverse.etl.metadata.JdbcMetadata";
    
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
        return "Generic Jdbc";
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
     * @see com.toolsverse.etl.driver.Driver#supportsScripts()
     */
    public boolean supportsScripts()
    {
        return true;
    }
    
}
