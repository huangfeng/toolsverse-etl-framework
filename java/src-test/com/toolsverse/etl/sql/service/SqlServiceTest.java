/*
 * SqlServiceTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.driver.GenericJdbcDriver;
import com.toolsverse.etl.parser.GenericSqlParser;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.config.SqlConfig;
import com.toolsverse.etl.sql.connection.AliasSessionConnectionProvider;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * SqlServiceTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlServiceTest
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
        
        SystemConfig.instance().bind(SqlService.class.getName(),
                SqlServiceImpl.class.getName());
    }
    
    @Test
    public void testExecuteMultipleStatemnts()
        throws Exception
    {
        String sql = "select * from users;select * from roles;";
        
        SqlRequest sqlRequest = new SqlRequest(
                new AliasSessionConnectionProvider(), SqlConfig.instance()
                        .getAlias("system"), new GenericSqlParser(),
                new GenericJdbcDriver(), null, sql, false, false, true);
        
        SqlService sqlService = (SqlService)ObjectFactory.instance().get(
                SqlService.class.getName());
        
        SqlResult result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() == null);
        
        assertNotNull(result.getDataSets());
        
        assertTrue(result.getDataSets().size() == 2);
        
        for (String name : result.getDataSets().keySet())
        {
            DataSet dataSet = result.getDataSets().get(name);
            
            assertNotNull(dataSet);
            
            assertTrue(dataSet.getRecordCount() > 0);
            
        }
    }
    
    @Test
    public void testExecuteSingleStatement()
        throws Exception
    {
        String sql = "select * from users";
        
        SqlRequest sqlRequest = new SqlRequest(
                new AliasSessionConnectionProvider(), SqlConfig.instance()
                        .getAlias("system"), new GenericSqlParser(),
                new GenericJdbcDriver(), null, sql, false, false, true);
        
        SqlService sqlService = (SqlService)ObjectFactory.instance().get(
                SqlService.class.getName());
        
        SqlResult result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() == null);
        
        assertNotNull(result.getDataSets());
        
        assertTrue(result.getDataSets().size() == 1);
        
        assertTrue(result.getDataSets().containsKey(
                EtlResource.DATASET_MSG.getValue() + "1"));
        
        DataSet dataSet = result.getDataSets().get(
                EtlResource.DATASET_MSG.getValue() + "1");
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() > 0);
    }
    
    @Test
    public void testExecuteSingleStatementWithMaxLimit()
        throws Exception
    {
        String sql = "select * from users";
        
        SqlRequest sqlRequest = new SqlRequest(
                new AliasSessionConnectionProvider(), SqlConfig.instance()
                        .getAlias("system"), new GenericSqlParser(),
                new GenericJdbcDriver(), null, sql, false, false, true, 0);
        
        SqlService sqlService = (SqlService)ObjectFactory.instance().get(
                SqlService.class.getName());
        
        SqlResult result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() == null);
        
        assertNotNull(result.getDataSets());
        
        assertTrue(result.getDataSets().size() == 1);
        
        assertTrue(result.getDataSets().containsKey(
                EtlResource.DATASET_MSG.getValue() + "1"));
        
        DataSet dataSet = result.getDataSets().get(
                EtlResource.DATASET_MSG.getValue() + "1");
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 0);
    }
    
    @Test
    public void testExecuteSingleStatementWithParams()
        throws Exception
    {
        String sql = "select * from users where user_id = :user_id";
        
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("user_id", "admin");
        
        SqlRequest sqlRequest = new SqlRequest(
                new AliasSessionConnectionProvider(), SqlConfig.instance()
                        .getAlias("system"), new GenericSqlParser(),
                new GenericJdbcDriver(), params, sql, true, false, true);
        
        SqlService sqlService = (SqlService)ObjectFactory.instance().get(
                SqlService.class.getName());
        
        SqlResult result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() == null);
        
        assertNotNull(result.getDataSets());
        
        assertTrue(result.getDataSets().size() == 1);
        
        assertTrue(result.getDataSets().containsKey(
                EtlResource.DATASET_MSG.getValue() + "1"));
        
        DataSet dataSet = result.getDataSets().get(
                EtlResource.DATASET_MSG.getValue() + "1");
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 1);
        
        params = new HashMap<String, Object>();
        params.put("user_id", "dfskdsf");
        
        sqlRequest = new SqlRequest(new AliasSessionConnectionProvider(),
                SqlConfig.instance().getAlias("system"),
                new GenericSqlParser(), new GenericJdbcDriver(), params, sql,
                true, false, true);
        
        result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() == null);
        
        assertNotNull(result.getDataSets());
        
        assertTrue(result.getDataSets().size() == 1);
        
        assertTrue(result.getDataSets().containsKey(
                EtlResource.DATASET_MSG.getValue() + "1"));
        
        dataSet = result.getDataSets().get(
                EtlResource.DATASET_MSG.getValue() + "1");
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 0);
    }
    
    @Test
    public void testExecuteWithException()
        throws Exception
    {
        String sql = "sql which doesn't have any sense";
        
        SqlRequest sqlRequest = new SqlRequest(
                new AliasSessionConnectionProvider(), SqlConfig.instance()
                        .getAlias("system"), new GenericSqlParser(),
                new GenericJdbcDriver(), null, sql, false, false, true);
        
        SqlService sqlService = (SqlService)ObjectFactory.instance().get(
                SqlService.class.getName());
        
        SqlResult result = sqlService.executeSql(sqlRequest);
        
        assertNotNull(result);
        
        assertTrue(result.getException() != null);
    }
    
}