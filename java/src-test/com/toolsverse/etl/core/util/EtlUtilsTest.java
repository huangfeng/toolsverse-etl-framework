/*
 * EtlUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.MemoryCache;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.CommonEtlUtils;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetGenerator;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.MockCallableDriver;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * EtlFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlUtilsTest
{
    private static final String TEST_EXCEPTION = "Exception occured: java.sql.SQLException: ORA-06550: line 4, column 4:\n"
            + "PLS-00201: identifier 'TEST' must be declared\n"
            + "ORA-06550: line 4, column 4:\n" + "PL/SQL: Statement ignored";
    
    private static final String TEST_CODE = "test(123);";
    
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
    public void testFileName()
    {
        assertTrue("unknown.xml".equals(EtlUtils.getFileName("")));
        
        assertTrue("test.xml".equals(EtlUtils
                .getFileName("c:/test/scenario/test.xml")));
        
        assertTrue("test.xml".equals(EtlUtils
                .getFileName("c:/test/scenario/test")));
        
        assertTrue("test.xml".equals(EtlUtils.getFileName("test.xml")));
        
        assertTrue("test.xml".equals(EtlUtils.getFileName("test")));
    }
    
    @Test
    public void testGetDbId()
    {
        Alias alias = new Alias();
        
        alias.setUrl("jdbc:sqlserver://localhost:1433;DatabaseName=etl;");
        
        String dbId = EtlUtils.getDbId(alias, ";");
        
        assertTrue("DatabaseName=etl".equals(dbId));
    }
    
    @Test
    public void testGetKey()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        
        Map<String, FieldDef> keys = CommonEtlUtils.getKeyFields(
                DataSetGenerator.FIELD1_NAME + ","
                        + DataSetGenerator.FIELD2_NAME, dataSet.getFields());
        
        assertTrue(!(DataSetGenerator.FIELD1_RECORD2_VALUE + DataSetGenerator.FIELD2_RECORD2_VALUE)
                .equals(CommonEtlUtils.getKey(dataSet, dataSet.getRecord(1),
                        keys, true, true)));
        
        assertTrue((DataSetGenerator.FIELD1_RECORD2_VALUE + DataSetGenerator.FIELD2_RECORD2_VALUE)
                .toUpperCase().equals(
                        CommonEtlUtils.getKey(dataSet, dataSet.getRecord(1),
                                keys, true, true)));
        
        assertTrue((DataSetGenerator.FIELD1_RECORD2_VALUE + DataSetGenerator.FIELD2_RECORD2_VALUE)
                .equals(CommonEtlUtils.getKey(dataSet, dataSet.getRecord(1),
                        keys, false, true)));
        
        assertTrue("".equals(CommonEtlUtils.getKey(dataSet,
                dataSet.getRecord(1), new HashMap<String, FieldDef>(), true,
                true)));
        
    }
    
    @Test
    public void testGetKeyFields()
        throws Exception
    {
        assertTrue(CommonEtlUtils.getKeyFields(null, null).size() == 0);
        
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        
        assertTrue(CommonEtlUtils.getKeyFields("abc,xyz", dataSet.getFields())
                .size() == 0);
        
        assertTrue(CommonEtlUtils.getKeyFields(
                DataSetGenerator.FIELD2_NAME + ","
                        + DataSetGenerator.FIELD3_NAME, dataSet.getFields())
                .size() == 2);
        
        assertTrue(CommonEtlUtils.getKeyFields(DataSetGenerator.FIELD1_NAME,
                dataSet.getFields()).size() == 1);
        
    }
    
    @Test
    public void testGetPatternName()
    {
        assertTrue(EtlUtils.getPatternName(null) == null);
        
        assertTrue(EtlUtils.getPatternName("") == null);
        
        assertTrue(EtlUtils.getPatternName(" ") == null);
        
        assertTrue(EtlUtils.getPatternName("test").equals("{test}"));
    }
    
    @Test
    public void testGetSqlFileName()
    {
        assertTrue(EtlUtils.getSqlFileName(null, 0) == null);
        
        assertTrue(EtlUtils.getSqlFileName("", 0) == null);
        
        assertTrue(EtlUtils.getSqlFileName(" ", 0) == null);
        
        assertTrue(EtlUtils.getSqlFileName("test", -1).equals(
                SystemConfig.instance().getScriptsFolder() + "test.sql"));
        
        assertTrue(EtlUtils.getSqlFileName("test", 1).equals(
                SystemConfig.instance().getScriptsFolder() + "test_1.sql"));
        
        assertTrue(EtlUtils.getSqlFileName("test", 2).equals(
                SystemConfig.instance().getScriptsFolder() + "test_2.sql"));
    }
    
    @Test
    public void testInsert2Update()
    {
        Driver driver = new MockCallableDriver();
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("field1");
        fields.add("field2");
        fields.add("field3");
        fields.add("field4");
        
        List<String> values = new ArrayList<String>();
        
        values.add("1");
        values.add("2");
        values.add("3");
        values.add("4");
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = new TypedKeyValue<List<String>, List<String>>(
                fields, values);
        
        String sql = driver.getInsertStatement(fieldsAndValues, "test");
        
        assertTrue("update test set \nfield3=3,\nfield4=4\nwhere field1=1 and field2=2"
                .equals(EtlUtils.insert2Update(sql, "field1,field2")));
        
        assertTrue("update test set \nfield2=2,\nfield3=3,\nfield4=4\nwhere field1=1"
                .equals(EtlUtils.insert2Update(sql, "field1")));
        
    }
    
    @Test
    public void testMergeSqlWithVars()
    {
        Cache<String, Object> cache = new MemoryCache<String, Object>();
        Map<String, Object> bindVars = new HashMap<String, Object>();
        ListHashMap<String, Variable> variables = new ListHashMap<String, Variable>();
        
        Variable var = new Variable();
        var.setName("abc");
        var.setValue("john");
        variables.put(var.getName(), var);
        
        var = new Variable();
        var.setName("xyz");
        var.setValue("smith");
        variables.put(var.getName(), var);
        
        String sql = "select * from dual where first_name={abc} and last_name={xyz}";
        String using = "abc,xyz";
        
        sql = EtlUtils.mergeSqlWithVars(cache, sql, using, variables, bindVars);
        
        assertTrue("select * from dual where first_name=? and last_name=?"
                .equals(sql));
        
        sql = "select * from dual where first_name='{abc}' and last_name='{xyz}'";
        using = null;
        
        sql = EtlUtils.mergeSqlWithVars(cache, sql, using, variables, bindVars);
        
        assertTrue("select * from dual where first_name='john' and last_name='smith'"
                .equals(sql));
        
        sql = "select * from dual where first_name={abc} and last_name={xyz}";
        using = "first_name,last_name";
        
        sql = EtlUtils.mergeSqlWithVars(cache, sql, using, variables, bindVars);
        
        assertTrue("select * from dual where first_name=john and last_name=smith"
                .equals(sql));
        
    }
    
    @Test
    public void testParseException()
    {
        assertTrue(4 == EtlUtils.parseException(new Exception(TEST_EXCEPTION),
                TEST_CODE, " line "));
        
        assertTrue(-1 == EtlUtils.parseException(null, TEST_CODE, " line "));
        
        assertTrue(-1 == EtlUtils.parseException(new Exception(""), TEST_CODE,
                " line "));
        
        assertTrue(-1 == EtlUtils.parseException(new Exception(TEST_EXCEPTION),
                null, " line "));
        
        assertTrue(-1 == EtlUtils.parseException(new Exception(TEST_EXCEPTION),
                TEST_CODE, null));
    }
    
    @Test
    public void testScenarioFileName()
    {
        assertTrue("c:/test/scenario/test.xml".equals(EtlUtils
                .getScenarioFileName("c:/test/scenario/", "test.xml")));
        
        assertTrue("c:/test/scenario/test.xml".equals(EtlUtils
                .getScenarioFileName("c:/test/scenario/abc",
                        "c:/test/scenario/test.xml")));
        
        assertTrue("c:/test/scenario/test.xml".equals(EtlUtils
                .getScenarioFileName("", "c:/test/scenario/test.xml")));
        
        assertTrue("c:/test/scenario/test.xml".equals(EtlUtils
                .getScenarioFileName("", "c:/test/scenario/test")));
        
        assertTrue("c:/test/scenario/test.xml".equals(EtlUtils
                .getScenarioFileName("c:/test/scenario/", "test")));
    }
    
    @Test
    public void testScenarioName()
    {
        assertTrue("test".equals(EtlUtils
                .getScenarioName("c:/test/scenario/test.xml")));
        
        assertTrue("test".equals(EtlUtils.getScenarioName("test.xml")));
        
        assertTrue("test".equals(EtlUtils
                .getScenarioName("c:/test/scenario/test")));
        
    }
    
    @Test
    public void testVars()
    {
        ListHashMap<String, Variable> vars = new ListHashMap<String, Variable>();
        
        EtlUtils.addVar(vars, "test", "123", false);
        
        assertTrue(vars.containsKey("test"));
        assertTrue("123".equals(vars.get("test").getValue()));
        
        EtlUtils.addVar(vars, "test", "456", true);
        assertTrue("456".equals(vars.get("test").getValue()));
        
        assertTrue("456".equals(EtlUtils.getVarValue(vars, "test", "123")));
        
        assertTrue("123".equals(EtlUtils.getVarValue(null, "test", "123")));
        
        assertTrue("123".equals(EtlUtils.getVarValue(vars, "xxx", "123")));
    }
    
}
