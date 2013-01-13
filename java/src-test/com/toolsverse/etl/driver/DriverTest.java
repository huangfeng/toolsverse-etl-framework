/*
 * DriverTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * DriverTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DriverTest
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
    public void testConvertStringForStorage()
        throws Exception
    {
        Driver driver = new GenericJdbcDriver();
        
        assertNotNull("'abc'".equals(driver.convertStringForStorage("abc")));
        
        int sizePrev = driver.getMaxStringLiteralSize();
        
        MockCallableDriver callableDriver = new MockCallableDriver();
        
        driver.setParentDriverName(callableDriver.getClass().getName());
        
        int sizeAfter = driver.getMaxStringLiteralSize();
        
        assertTrue(sizePrev != sizeAfter);
        
        assertTrue(sizeAfter > 0);
        
        String test = Utils.padding(sizeAfter + 1, 'a');
        
        assertTrue(sizeAfter < test.length());
        
        assertTrue(("'" + test.substring(0, test.length() - 1) + "'")
                .equals(driver.convertStringForStorage(test)));
    }
    
    @Test
    public void testConvertValueForStorage()
        throws Exception
    {
        Driver driver = new GenericJdbcDriver();
        
        assertTrue("NULL".equalsIgnoreCase(driver.convertValueForStorage(null,
                Types.INTEGER, true)));
        
        assertTrue("'abc'".equalsIgnoreCase(driver.convertValueForStorage(
                "abc", Types.VARCHAR, true)));
        
        assertTrue("'01/01/2001'".equalsIgnoreCase(driver
                .convertValueForStorage("01/01/2001", Types.DATE, true)));
        
        Date test = new Date();
        
        String value = Utils.date2Str(test, DataSet.DATA_SET_DATE_ONLY_FORMAT);
        
        assertTrue(("'" + value + "'").equalsIgnoreCase(driver
                .convertValueForStorage(test, Types.DATE, true)));
    }
    
    @Test
    public void testFilter()
    {
        Driver driver = new GenericJdbcDriver();
        
        assertTrue("a''bcd''ef".equals(driver.filter("a'bcd'ef")));
    }
    
    @Test
    public void testGetDefaultFunctionClass()
    {
        Driver driver = new GenericJdbcDriver();
        
        assertTrue(GenericJdbcDriver.DEF_FUNCTION_CLASS.equals(driver
                .getDefaultFunctionClass()));
    }
    
    @Test
    public void testGetDeleteStatement()
        throws Exception
    {
        Driver driver = new MockCallableDriver();
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("field1");
        fields.add("field2");
        fields.add("field3");
        fields.add("field4");
        
        List<String> values = new ArrayList<String>();
        
        values.add("123");
        values.add("'abc'");
        values.add("sysdate");
        values.add("17");
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = new TypedKeyValue<List<String>, List<String>>(
                fields, values);
        
        String sql = driver.getDeleteStatement(fieldsAndValues, "test",
                "field2, field3");
        
        String update = "delete from test\nwhere field2='abc' and field3=sysdate;\n";
        
        assertTrue(update.equals(sql));
        
        fields.clear();
        values.clear();
        
        fields.add("field1");
        fields.add("field2");
        
        values.add("123");
        values.add("'abc'");
        
        sql = driver.getDeleteStatement(fieldsAndValues, "test", "field1");
        
        update = "delete from test\nwhere field1=123;\n";
        
        assertTrue(update.equals(sql));
        
        sql = driver.getDeleteStatement(fieldsAndValues, "test", "");
        
        assertTrue("".equals(sql));
        
        fields.clear();
        
        sql = driver.getDeleteStatement(fieldsAndValues, "test",
                "field2, field3");
        
        assertTrue("".equals(sql));
    }
    
    @Test
    public void testGetInsertStatement()
        throws Exception
    {
        Driver driver = new MockCallableDriver();
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("field1");
        fields.add("field2");
        
        List<String> values = new ArrayList<String>();
        
        values.add("123");
        values.add("'abc'");
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = new TypedKeyValue<List<String>, List<String>>(
                fields, values);
        
        String sql = driver.getInsertStatement(fieldsAndValues, "test");
        
        String insert = "insert into test(\nfield1,\nfield2\n) values (\n123,\n'abc'\n);\n";
        
        assertTrue(insert.equals(sql));
        
        fields.clear();
        
        sql = driver.getInsertStatement(fieldsAndValues, "test");
        
        assertTrue("".equals(sql));
    }
    
    @Test
    public void testGetMergeStatementDefault()
        throws Exception
    {
        Driver driver = new MockCallableDriver();
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("field1");
        fields.add("field2");
        fields.add("field3");
        fields.add("field4");
        
        List<String> values = new ArrayList<String>();
        
        values.add("123");
        values.add("'abc'");
        values.add("sysdate");
        values.add("17");
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = new TypedKeyValue<List<String>, List<String>>(
                fields, values);
        
        String sql = driver.getMergeStatement(fieldsAndValues, "test",
                "field2, field3");
        
        String merge = "if (not exists (select 1 from test where field2='abc' and field3=sysdate)) then\ninsert into test(\nfield1,\nfield2,\nfield3,\nfield4\n) values (\n123,\n'abc',\nsysdate,\n17\n);\n else\nupdate test\nset field1=123\n,field4=17\nwhere field2='abc' and field3=sysdate;\n end if;\n";
        
        assertTrue(merge.equals(sql));
        
        fields.clear();
        values.clear();
        
        fields.add("field1");
        fields.add("field2");
        
        values.add("123");
        values.add("'abc'");
        
        sql = driver.getMergeStatement(fieldsAndValues, "test", "field1");
        
        merge = "if (not exists (select 1 from test where field1=123)) then\ninsert into test(\nfield1,\nfield2\n) values (\n123,\n'abc'\n);\n else\nupdate test\nset field2='abc'\nwhere field1=123;\n end if;\n";
        
        assertTrue(merge.equals(sql));
        
        sql = driver.getMergeStatement(fieldsAndValues, "test", "");
        
        assertTrue("".equals(sql));
        
        fields.clear();
        
        sql = driver.getMergeStatement(fieldsAndValues, "test",
                "field2, field3");
        
        assertTrue("".equals(sql));
    }
    
    @Test
    public void testGetParentDriver()
        throws Exception
    {
        Driver driver = new GenericJdbcDriver();
        
        assertNull(driver.getParentDriverName());
        
        Exception e = null;
        
        try
        {
            driver.getMaxCharSize();
        }
        catch (IllegalArgumentException ex)
        {
            e = ex;
        }
        
        assertNotNull(e);
        
        e = null;
        try
        {
            driver.getMaxPrecision();
        }
        catch (IllegalArgumentException ex)
        {
            e = ex;
        }
        
        assertNotNull(e);
        
        e = null;
        try
        {
            driver.getMaxScale();
        }
        catch (IllegalArgumentException ex)
        {
            e = ex;
        }
        
        assertNotNull(e);
        
        e = null;
        try
        {
            driver.getMaxVarcharSize();
        }
        catch (IllegalArgumentException ex)
        {
            e = ex;
        }
        
        assertNotNull(e);
        
        MockCallableDriver callableDriver = new MockCallableDriver();
        
        driver.setParentDriverName(MockCallableDriver.class.getName());
        
        assertTrue(callableDriver.getClass().getName()
                .equals(driver.getParentDriverName()));
        
        assertTrue(driver.getMaxCharSize() == callableDriver.getMaxCharSize());
        
        assertTrue(driver.getMaxPrecision() == callableDriver.getMaxPrecision());
        
        assertTrue(driver.getMaxScale() == callableDriver.getMaxScale());
        
        assertTrue(driver.getMaxStringLiteralSize() == callableDriver
                .getMaxStringLiteralSize());
        
        assertTrue(driver.getMaxVarcharSize() == callableDriver
                .getMaxVarcharSize());
    }
    
    @Test
    public void testGetType()
        throws Exception
    {
        Driver driver = new GenericJdbcDriver();
        
        FieldDef fieldDef = new FieldDef();
        
        fieldDef.setSqlDataType(Types.INTEGER);
        
        assertTrue("NUMBER".equals(driver.getType(fieldDef, driver.getClass()
                .getName(), null)));
        
        fieldDef.setSqlDataType(SqlUtils.LONGNVARCHAR);
        
        assertTrue("VARCHAR".equals(driver.getType(fieldDef, driver.getClass()
                .getName(), null)));
        
        fieldDef.setSqlDataType(Types.VARBINARY);
        
        assertTrue("BLOB".equals(driver.getType(fieldDef, driver.getClass()
                .getName(), null)));
        
        FieldsRepository fieldsRepository = new FieldsRepository()
        {
            public List<FieldDef> getFieldDef(String key, int type)
            {
                return null;
            }
        };
        
        fieldDef.setSqlDataType(SqlUtils.LONGNVARCHAR);
        
        assertTrue("VARCHAR".equals(driver.getType(fieldDef, driver.getClass()
                .getName(), fieldsRepository)));
        
        fieldsRepository = new FieldsRepository()
        {
            public List<FieldDef> getFieldDef(String key, int type)
            {
                List<FieldDef> list = new ArrayList<FieldDef>();
                
                return list;
            }
        };
        
        fieldDef.setSqlDataType(Types.VARCHAR);
        fieldDef.setNativeDataType("VARCHAR(100)");
        
        fieldsRepository = new FieldsRepository()
        {
            public List<FieldDef> getFieldDef(String key, int type)
            {
                List<FieldDef> list = new ArrayList<FieldDef>();
                
                FieldDef field = new FieldDef();
                
                field.setSqlDataType(Types.VARCHAR);
                field.setNativeDataType("VARCHAR2");
                
                list.add(field);
                
                return list;
            }
        };
        
        assertTrue("VARCHAR2".equals(driver.getType(fieldDef, driver.getClass()
                .getName(), fieldsRepository)));
        
        fieldDef.setSqlDataType(Types.VARCHAR);
        fieldDef.setNativeDataType("VARCHAR(200)");
        fieldDef.setPrecision(200);
        fieldDef.setHasParams(true);
        
        fieldsRepository = new FieldsRepository()
        {
            public List<FieldDef> getFieldDef(String key, int type)
            {
                List<FieldDef> list = new ArrayList<FieldDef>();
                
                FieldDef field = new FieldDef();
                
                field.setSqlDataType(Types.VARCHAR);
                field.setNativeDataType("VARCHAR2");
                field.setPrecision(100);
                field.setHasParams(true);
                
                list.add(field);
                
                return list;
            }
        };
        
        assertTrue("VARCHAR2(100)".equals(driver.getType(fieldDef, driver
                .getClass().getName(), fieldsRepository)));
        
        fieldDef.setSqlDataType(Types.VARCHAR);
        fieldDef.setNativeDataType("VARCHAR(200)");
        fieldDef.setPrecision(200);
        fieldDef.setHasParams(true);
        
        fieldsRepository = new FieldsRepository()
        {
            public List<FieldDef> getFieldDef(String key, int type)
            {
                List<FieldDef> list = new ArrayList<FieldDef>();
                
                FieldDef field = new FieldDef();
                
                field.setSqlDataType(Types.VARCHAR);
                field.setNativeDataType("VARCHAR2");
                field.setHasParams(true);
                
                list.add(field);
                
                return list;
            }
        };
        
        assertTrue("VARCHAR2(200)".equals(driver.getType(fieldDef, driver
                .getClass().getName(), fieldsRepository)));
    }
    
    @Test
    public void testGetUpdateStatement()
        throws Exception
    {
        Driver driver = new MockCallableDriver();
        
        List<String> fields = new ArrayList<String>();
        
        fields.add("field1");
        fields.add("field2");
        fields.add("field3");
        fields.add("field4");
        
        List<String> values = new ArrayList<String>();
        
        values.add("123");
        values.add("'abc'");
        values.add("sysdate");
        values.add("17");
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = new TypedKeyValue<List<String>, List<String>>(
                fields, values);
        
        String sql = driver.getUpdateStatement(fieldsAndValues, "test",
                "field2, field3");
        
        String update = "update test\nset field1=123\n,field4=17\nwhere field2='abc' and field3=sysdate;\n";
        
        assertTrue(update.equals(sql));
        
        fields.clear();
        values.clear();
        
        fields.add("field1");
        fields.add("field2");
        
        values.add("123");
        values.add("'abc'");
        
        sql = driver.getUpdateStatement(fieldsAndValues, "test", "field1");
        
        update = "update test\nset field2='abc'\nwhere field1=123;\n";
        
        assertTrue(update.equals(sql));
        
        sql = driver.getUpdateStatement(fieldsAndValues, "test", "");
        
        assertTrue("".equals(sql));
        
        fields.clear();
        
        sql = driver.getUpdateStatement(fieldsAndValues, "test",
                "field2, field3");
        
        assertTrue("".equals(sql));
    }
    
}