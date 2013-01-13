/*
 * SqlConfigTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.config;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * SqlConfigTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlConfigTest
{
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());

        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);

        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    @Test
    public void testGetAlias()
        throws Exception
    {
        SqlConfig sqlConfig = SqlConfig.instance();
        
        Alias alias = sqlConfig.getAlias("system");
        
        assertNotNull(alias);
        
        assertTrue("system".equals(alias.getName()));
        
        assertTrue("org.apache.derby.jdbc.EmbeddedDriver".equals(alias
                .getJdbcDriverClass()));
    }
    
    @Test
    public void testInit()
        throws Exception
    {
        SqlConfig sqlConfig = SqlConfig.instance();
        
        assertNotNull(sqlConfig);
    }
}
