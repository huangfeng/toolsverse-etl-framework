/*
 * QedMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata.qed;

import java.util.ArrayList;
import java.util.List;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.metadata.JdbcMetadata;

/**
 * The QED implementation of the Metadata interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class QedMetadata extends JdbcMetadata
{
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.JdbcMetadata#getDbObjectTypes()
     */
    @Override
    public List<String> getDbObjectTypes()
        throws Exception
    {
        List<String> types = getTableTypes();
        
        return types;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
     */
    @Override
    public String getLicensePropertyName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.JdbcMetadata#getTableTypes()
     */
    @Override
    public List<String> getTableTypes()
        throws Exception
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_TABLES);
        list.add(TYPE_VIEWS);
        
        return list;
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
    
}
