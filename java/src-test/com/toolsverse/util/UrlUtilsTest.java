/*
 * UrlUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * UrlUtilsTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class UrlUtilsTest
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
    public void testDecodeUrl()
    {
        assertTrue(UrlUtils.decodeUrl("http://abc%20/xyz%20/index.html")
                .equals("http://abc /xyz /index.html"));
        
        assertTrue(UrlUtils.decodeUrl("%20%20").equals("  "));
        
        assertTrue(UrlUtils.decodeUrl(null) == null);
    }
    
    @Test
    public void testUrlToPath()
    {
        assertTrue(UrlUtils.urlToPath(null) == null);
        
        String path = UrlUtils
                .urlToPath("file:/C:/Tomcat 5.5/webapps/developer/WEB-INF/lib/toolsverse-core.jar!/com/toolsverse/config/SystemConfig.class");
        
        assertTrue("C:/Tomcat 5.5/webapps/developer/WEB-INF".equals(path));
        
        path = UrlUtils
                .urlToPath("C:/Tomcat 5.5/webapps/developer/WEB-INF/lib/toolsverse-core.jar!/com/toolsverse/config/SystemConfig.class");
        
        assertTrue("C:/Tomcat 5.5/webapps/developer/WEB-INF".equals(path));
        
        assertTrue(UrlUtils.urlToPath("c:/test") == null);
    }
    
    @Test
    public void testUrlUtils()
    {
        String url = "http://localhost:8080/something/{file:test}";
        
        assertTrue(UrlUtils.hasUrlToken(UrlUtils.FILE_TOKEN, url));
        
        assertTrue(UrlUtils.getFileName(UrlUtils.FILE_TOKEN, url)
                .equals("test"));
        
        assertTrue(UrlUtils.getUrl(UrlUtils.FILE_TOKEN, url).equals(
                "http://localhost:8080/something/test"));
        
        assertTrue(UrlUtils.setUrl(UrlUtils.FILE_TOKEN, url, "test2").equals(
                "http://localhost:8080/something/{file:test2}"));
    }
    
}
