/*
 * SqlUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetGenerator;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.GenericJdbcDriver;
import com.toolsverse.etl.sql.connection.AliasConnectionProvider;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * EtlFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlUtilsTest
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
    public void testAddBindVar()
    {
        assertTrue(!SqlUtils.addBindVar(null, null, null, null));
        
        Map<String, Object> bindVars = new HashMap<String, Object>();
        
        assertTrue(SqlUtils.addBindVar(new String[] {"abc", "col", "xyz"},
                "col", Boolean.TRUE, bindVars));
        assertTrue(bindVars.size() == 1);
        assertTrue(Boolean.TRUE.equals(bindVars.get("1")));
        
        assertTrue(SqlUtils.addBindVar(new String[] {"abc", "col", "xyz"},
                "abc", "test", bindVars));
        assertTrue(bindVars.size() == 2);
        assertTrue("test".equals(bindVars.get("0")));
    }
    
    @Test
    public void testAlreadyFormatedType()
    {
        assertTrue(SqlUtils.alreadyFormatedType("SOMETHING (max) DATA"));
        
        assertTrue(SqlUtils.alreadyFormatedType(null) == true);
        
        assertTrue(!SqlUtils.alreadyFormatedType("SOMETHING () DATA"));
        
        assertTrue(SqlUtils.alreadyFormatedType("NUMBER(100)"));
        
        assertTrue(!SqlUtils.alreadyFormatedType("NUMBER()"));
        
        assertTrue(SqlUtils.alreadyFormatedType("NUMBER"));
    }
    
    @Test
    public void testBaseName2Name()
    {
        assertTrue(SqlUtils.baseName2Name(null) == null);
        
        assertTrue(SqlUtils.baseName2Name(" ") == null);
        
        assertTrue("table1$"
                .equals(SqlUtils
                        .baseName2Name("C:/projects/toolsverse/java/data-test/test.table1$")));
        
        assertTrue("DATA_SOURCE".equals(SqlUtils
                .baseName2Name("APP.ALL.DATA_SOURCE")));
        
        assertTrue("DATA_SOURCE".equals(SqlUtils.baseName2Name("DATA_SOURCE")));
    }
    
    @Test
    public void testBlobTypeCheck()
    {
        assertTrue(!SqlUtils.isBlob(Types.CLOB));
        
        assertTrue(SqlUtils.isBlob(Types.BLOB));
    }
    
    @Test
    public void testBooleanTypeCheck()
    {
        assertTrue(SqlUtils.isBoolean(Types.BOOLEAN));
        
        assertTrue(!SqlUtils.isBoolean(Types.VARCHAR));
    }
    
    @Test
    public void testCharTypeCheck()
    {
        assertTrue(SqlUtils.isChar(Types.CHAR));
        
        assertTrue(SqlUtils.isChar(Types.VARCHAR));
        
        assertTrue(!SqlUtils.isChar(Types.CLOB));
        
        assertTrue(!SqlUtils.isChar(Types.BLOB));
    }
    
    @Test
    public void testClobTypeCheck()
    {
        assertTrue(SqlUtils.isClob(Types.CLOB));
        
        assertTrue(!SqlUtils.isClob(Types.BLOB));
    }
    
    @Test
    public void testConvertDataType()
    {
        FieldDef source = new FieldDef();
        source.setNativeDataType("VARCHAR(100)");
        source.setSqlDataType(Types.VARCHAR);
        
        FieldDef dest = new FieldDef();
        dest.setNativeDataType("VARCHAR2");
        dest.setSqlDataType(Types.VARCHAR);
        dest.setPrecision(255);
        dest.setHasParams(true);
        
        assertTrue(SqlUtils.convertDataType(null, dest) == null);
        
        assertTrue("VARCHAR(100)"
                .equals(SqlUtils.convertDataType(source, null)));
        
        assertTrue("VARCHAR2(100)".equals(SqlUtils
                .convertDataType(source, dest)));
        
        dest = new FieldDef();
        dest.setNativeDataType("VARCHAR2");
        dest.setSqlDataType(Types.VARCHAR);
        dest.setHasParams(true);
        dest.setPrecision(55);
        assertTrue("VARCHAR2(55)"
                .equals(SqlUtils.convertDataType(source, dest)));
        
        source = new FieldDef();
        source.setNativeDataType("NUMBER(20)");
        source.setSqlDataType(Types.NUMERIC);
        
        dest = new FieldDef();
        dest.setNativeDataType("DECIMAL");
        dest.setSqlDataType(Types.DECIMAL);
        dest.setPrecision(18);
        dest.setHasParams(true);
        
        assertTrue("DECIMAL(18)".equals(SqlUtils.convertDataType(source, dest)));
        
        dest = new FieldDef();
        dest.setNativeDataType("DECIMAL");
        dest.setSqlDataType(Types.DECIMAL);
        dest.setPrecision(18);
        dest.setHasParams(false);
        assertTrue("DECIMAL".equals(SqlUtils.convertDataType(source, dest)));
        
        source = new FieldDef();
        source.setNativeDataType("NUMBER(20, 5)");
        source.setSqlDataType(Types.NUMERIC);
        
        dest = new FieldDef();
        dest.setNativeDataType("DECIMAL");
        dest.setSqlDataType(Types.DECIMAL);
        dest.setPrecision(18);
        dest.setScale(4);
        dest.setHasParams(true);
        
        assertTrue("DECIMAL(18,4)".equals(SqlUtils
                .convertDataType(source, dest)));
        
        source = new FieldDef();
        source.setNativeDataType("BLOB");
        source.setSqlDataType(Types.BLOB);
        
        dest = new FieldDef();
        dest.setNativeDataType("IMAGE");
        dest.setSqlDataType(Types.BLOB);
        dest.setHasParams(false);
        
        assertTrue("IMAGE".equals(SqlUtils.convertDataType(source, dest)));
        
        source = new FieldDef();
        source.setNativeDataType("BLOB");
        source.setSqlDataType(Types.BLOB);
        
        dest = new FieldDef();
        dest.setNativeDataType("VARCHAR () FOR BIT");
        dest.setSqlDataType(Types.VARBINARY);
        dest.setHasParams(true);
        dest.setPrecision(10000);
        
        assertTrue("VARCHAR (10000) FOR BIT".equals(SqlUtils.convertDataType(
                source, dest)));
        
    }
    
    @Test
    public void testDate2Str()
    {
        assertTrue("01/01/2001 00:00:00".equals(SqlUtils.dateTime2Str(
                "01/01/2001 00:00:00", null)));
        
        java.util.Date date = Utils.str2Date("02/02/2002 00:00:00", null,
                "dd/MM/yyyy HH:mm:ss");
        assertTrue("2002-02-02 00:00:00".equals(SqlUtils.dateTime2Str(date,
                null)));
        
        assertTrue("01/01/2001".equals(SqlUtils.date2Str("01/01/2001", null)));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.DATE_FORMAT_PROP, DataSet.DATA_SET_DATE_ONLY_FORMAT);
        
        date = Utils.str2Date("02/02/2002", null, "dd/MM/yyyy");
        assertTrue("2002-02-02".equals(SqlUtils.date2Str(date, params)));
        
        assertTrue("02:02:02".equals(SqlUtils.time2Str("02:02:02", null)));
        
        date = Utils.str2Date("02:02:02", null, "HH:mm:ss");
        assertTrue("02:02:02".equals(SqlUtils.time2Str(date, null)));
    }
    
    @Test
    public void testDateOnlyTypeCheck()
    {
        assertTrue(SqlUtils.isDateOnly(Types.DATE));
        
        assertTrue(!SqlUtils.isDateOnly(Types.TIME));
        
        assertTrue(!SqlUtils.isDateOnly(Types.TIMESTAMP));
        
        assertTrue(!SqlUtils.isDateOnly(Types.VARCHAR));
    }
    
    @Test
    public void testDateTimeTypeCheck()
    {
        assertTrue(SqlUtils.isDateTime(Types.TIMESTAMP));
        
        assertTrue(SqlUtils.isDateTime(Types.DATE));
        
        assertTrue(!SqlUtils.isDateTime(Types.TIME));
        
        assertTrue(!SqlUtils.isDate(Types.VARCHAR));
    }
    
    @Test
    public void testDateTypeCheck()
    {
        assertTrue(SqlUtils.isDate(Types.DATE));
        
        assertTrue(SqlUtils.isDate(Types.TIME));
        
        assertTrue(SqlUtils.isDate(Types.TIMESTAMP));
        
        assertTrue(!SqlUtils.isDate(Types.VARCHAR));
    }
    
    @Test
    public void testDecimal2Str()
    {
        assertTrue("123".equals(SqlUtils.decimal2Str(123, null)));
        
        assertTrue("123".equals(SqlUtils.decimal2Str(123.00, null)));
        
        assertTrue("123.12".equals(SqlUtils.decimal2Str(123.12, null)));
        
        assertTrue("-123.12".equals(SqlUtils.decimal2Str(-123.12, null)));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.DECIMAL_AS_INT_PROP, "false");
        
        assertTrue(String.valueOf(123.00).equals(
                SqlUtils.decimal2Str(123.00, params)));
    }
    
    @Test
    public void testDecimalTypeCheck()
    {
        assertTrue(!SqlUtils.isDecimal(Types.TIMESTAMP));
        
        assertTrue(SqlUtils.isDecimal(Types.FLOAT));
        
        assertTrue(SqlUtils.isDecimal(Types.DOUBLE));
        
        assertTrue(SqlUtils.isDecimal(Types.NUMERIC));
        
        assertTrue(!SqlUtils.isDecimal(Types.INTEGER));
        
        assertTrue(SqlUtils.isDecimal(Types.DECIMAL));
    }
    
    @Test
    public void testFilter()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        String value = SqlUtils.filter(driver, "var xxx varchar(100):= '123'",
                Types.VARCHAR);
        
        assertTrue("var xxx varchar(100):= ''123''".equals(value));
    }
    
    @Test
    public void testFullName2BaseName()
    {
        assertTrue(SqlUtils.fullName2BaseName(null) == null);
        
        assertTrue(SqlUtils.fullName2BaseName(" ") == null);
        
        assertTrue("C:/projects/toolsverse/java/data-test/test.table1$"
                .equals(SqlUtils
                        .fullName2BaseName("Excel Test:C:/projects/toolsverse/java/data-test/test.table1$")));
        
        assertTrue("APP.DATA_SOURCE".equals(SqlUtils
                .fullName2BaseName("Java DB Test:APP.DATA_SOURCE")));
    }
    
    @Test
    public void testFullName2Name()
    {
        assertTrue(SqlUtils.fullName2Name(null) == null);
        
        assertTrue(SqlUtils.fullName2Name(" ") == null);
        
        assertTrue("table1$"
                .equals(SqlUtils
                        .fullName2Name("Excel Test:C:/projects/toolsverse/java/data-test/test.table1$")));
        
        assertTrue("DATA_SOURCE".equals(SqlUtils
                .fullName2Name("Java DB Test:APP.DATA_SOURCE")));
        
        assertTrue(SqlUtils.fullName2Name("Java DB Test:APP") == null);
    }
    
    @Test
    public void testFullName2NodeName()
    {
        assertTrue(SqlUtils.fullName2NodeName(null) == null);
        
        assertTrue(SqlUtils.fullName2NodeName(" ") == null);
        
        assertTrue("Excel Test"
                .equals(SqlUtils
                        .fullName2NodeName("Excel Test:C:/projects/toolsverse/java/data-test/test.table1$")));
    }
    
    @Test
    public void testFullName2OwnerName()
    {
        assertTrue(SqlUtils.fullName2OwnerName(null) == null);
        
        assertTrue(SqlUtils.fullName2OwnerName(" ") == null);
        
        assertTrue("C:/projects/toolsverse/java/data-test/test"
                .equals(SqlUtils
                        .fullName2OwnerName("Excel Test:C:/projects/toolsverse/java/data-test/test.table1$")));
        
        assertTrue("APP".equals(SqlUtils
                .fullName2OwnerName("Java DB Test:APP.DATA_SOURCE")));
        
        assertTrue("APP"
                .equals(SqlUtils.fullName2OwnerName("Java DB Test:APP")));
    }
    
    @Test
    public void testGetAfterFromFromSelect()
    {
        assertTrue("abc".equals(SqlUtils
                .getAfterFromFromSelect("select * from abc")));
        
        assertTrue("abc where a=b and c is null"
                .equals(SqlUtils
                        .getAfterFromFromSelect("select a, b,c,   d from abc where a=b and c is null ")));
        
        assertTrue("abc WHERE a=b and c is null order by xxx"
                .equals(SqlUtils
                        .getAfterFromFromSelect("seLECt a,B,c,D fROm abc WHERE a=b and c is null order by xxx")));
        
        assertTrue("".equals(SqlUtils.getAfterFromFromSelect("select *")));
        
        assertTrue("".equals(SqlUtils.getAfterFromFromSelect("")));
        
        assertNull(SqlUtils.getAfterFromFromSelect(null));
    }
    
    @Test
    public void testGetAfterWhereFromSelect()
    {
        assertTrue("".equals(SqlUtils
                .getAfterWhereFromSelect("select * from abc")));
        
        assertTrue("a=b and c is null"
                .equals(SqlUtils
                        .getAfterWhereFromSelect("select a, b,c,   d from abc where a=b and c is null ")));
        
        assertTrue("a=b and c is null order by xxx"
                .equals(SqlUtils
                        .getAfterWhereFromSelect("seLECt a,B,c,D fROm abc WHERE a=b and c is null order by xxx")));
        
        assertTrue("".equals(SqlUtils.getAfterWhereFromSelect("select *")));
        
        assertTrue("".equals(SqlUtils.getAfterWhereFromSelect("")));
        
        assertNull(SqlUtils.getAfterWhereFromSelect(null));
    }
    
    @Test
    public void testGetColName()
    {
        Map<String, Integer> cols = new HashMap<String, Integer>();
        
        assertTrue("col".equals(SqlUtils.getColName(cols, "col")));
        assertTrue("col1".equals(SqlUtils.getColName(cols, "col")));
        assertTrue("col2".equals(SqlUtils.getColName(cols, "col")));
        assertTrue("abc".equals(SqlUtils.getColName(cols, "abc")));
    }
    
    @Test
    public void testGetFieldAsText()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertTrue("test varchar(100)".equalsIgnoreCase(SqlUtils
                .getFieldAsText(driver, "test", "varchar", 100, null, "Yes",
                        Types.VARCHAR)));
        
        assertTrue("test clob not null".equalsIgnoreCase(SqlUtils
                .getFieldAsText(driver, "test", "clob", 100, null, false,
                        Types.CLOB)));
        
        assertTrue("test number(18)".equalsIgnoreCase(SqlUtils.getFieldAsText(
                driver, "test", "number", 18, 0, true, Types.NUMERIC)));
        
        assertTrue("test number(18,5)".equalsIgnoreCase(SqlUtils
                .getFieldAsText(driver, "test", "number", 18, 5, true,
                        Types.NUMERIC)));
    }
    
    @Test
    public void testGetFieldsFromSelect()
    {
        assertTrue("*"
                .equals(SqlUtils.getFieldsFromSelect("select * from abc")));
        
        assertTrue("a, b,c,   d".equals(SqlUtils
                .getFieldsFromSelect("select a, b,c,   d from abc")));
        
        assertTrue("a,B,c,D".equals(SqlUtils
                .getFieldsFromSelect("seLECt a,B,c,D fROm abc")));
        
        assertTrue("".equals(SqlUtils.getFieldsFromSelect("select *")));
        
        assertTrue("".equals(SqlUtils.getFieldsFromSelect("")));
        
        assertNull(SqlUtils.getFieldsFromSelect(null));
    }
    
    @Test
    public void testGetFieldsSql()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockCallableDriver", null, null);
        
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        
        String sql = SqlUtils.getFieldsSql(dataSet, driver, null, null);
        
        assertTrue("numfield  INT ,\nstrfield  VARCHAR(8000) ,\ndatetimefield  DATETIME ,\nintfield  INT ,\nfloatfield  FLOAT ,\ndatefield  DATETIME ,\ntimefield  DATETIME \n"
                .equals(sql));
    }
    
    @Test
    public void testGetFieldType()
    {
        assertTrue(SqlUtils.getFieldType(Types.INTEGER, Types.VARCHAR, false) == Types.INTEGER);
        
        assertTrue(SqlUtils.getFieldType(Types.FLOAT, Types.INTEGER, true) == Types.FLOAT);
        
        assertTrue(SqlUtils.getFieldType(Types.CHAR, Types.INTEGER, true) == Types.VARCHAR);
        
        assertTrue(SqlUtils.getFieldType(Types.CHAR, Types.INTEGER, false) == Types.CHAR);
    }
    
    @Test
    public void testGetFullNativeType()
    {
        FieldDef fieldDef = new FieldDef();
        fieldDef.setNativeDataType("SOMETHING (max) DATA");
        
        assertTrue("SOMETHING (max) DATA".equals(SqlUtils
                .getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setNativeDataType("NUMBER");
        
        assertTrue("NUMBER".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.NUMERIC);
        fieldDef.setNativeDataType("NUMBER");
        fieldDef.setPrecision(255);
        fieldDef.setHasParams(true);
        
        assertTrue("NUMBER(255)".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.VARBINARY);
        fieldDef.setNativeDataType("VARCHAR () FOR BIT DATA");
        fieldDef.setPrecision(1000);
        fieldDef.setHasParams(true);
        
        assertTrue("VARCHAR (1000) FOR BIT DATA".equals(SqlUtils
                .getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.CHAR);
        fieldDef.setNativeDataType("CHAR");
        fieldDef.setPrecision(100);
        fieldDef.setHasParams(true);
        
        assertTrue("CHAR(100)".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.NUMERIC);
        fieldDef.setNativeDataType("NUMBER");
        fieldDef.setPrecision(255);
        fieldDef.setHasParams(false);
        
        assertTrue("NUMBER".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.NUMERIC);
        fieldDef.setNativeDataType("NUMBER(200)");
        fieldDef.setPrecision(255);
        fieldDef.setHasParams(false);
        
        assertTrue("NUMBER(200)".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.CLOB);
        fieldDef.setNativeDataType("LONG VARCHAR");
        fieldDef.setPrecision(100000);
        fieldDef.setHasParams(false);
        
        assertTrue("LONG VARCHAR".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.CLOB);
        fieldDef.setNativeDataType("LONG (123) VARCHAR");
        fieldDef.setPrecision(-1);
        fieldDef.setHasParams(true);
        
        assertTrue("LONG VARCHAR".equals(SqlUtils.getFullNativeType(fieldDef)));
        
        fieldDef = new FieldDef();
        fieldDef.setSqlDataType(Types.NUMERIC);
        fieldDef.setNativeDataType("NUMBER(21,29)");
        fieldDef.setPrecision(10);
        fieldDef.setScale(17);
        fieldDef.setHasParams(true);
        
        assertTrue("NUMBER(10,17)".equals(SqlUtils.getFullNativeType(fieldDef)));
    }
    
    @Test
    public void testGetJustTypeName()
    {
        assertTrue("VARCHAR".equals(SqlUtils.getJustTypeName("VARCHAR(100)")));
        
        assertTrue("NUMBER".equals(SqlUtils.getJustTypeName("NUMBER(10, 20)")));
        
        assertTrue("INTEGER".equals(SqlUtils.getJustTypeName("INTEGER")));
    }
    
    @Test
    public void testGetNumberTypeAndValue()
    {
        TypedKeyValue<Integer, Number> typeAndValue = SqlUtils
                .getNumberTypeAndValue("abc");
        assertNull(typeAndValue);
        
        typeAndValue = SqlUtils.getNumberTypeAndValue("  ");
        assertNull(typeAndValue);
        
        SqlUtils.getNumberTypeAndValue("");
        assertNull(typeAndValue);
        
        SqlUtils.getNumberTypeAndValue(null);
        assertNull(typeAndValue);
        
        typeAndValue = SqlUtils.getNumberTypeAndValue("123");
        
        assertNotNull(typeAndValue);
        assertTrue(typeAndValue.getKey() == Types.INTEGER);
        assertTrue(typeAndValue.getValue() instanceof Integer);
        assertTrue((Integer)typeAndValue.getValue() == 123);
        
        typeAndValue = SqlUtils.getNumberTypeAndValue("123.12");
        
        assertNotNull(typeAndValue);
        assertTrue(typeAndValue.getKey() == Types.FLOAT);
        assertTrue(typeAndValue.getValue() instanceof Float);
        assertTrue((Float)typeAndValue.getValue() == (float)123.12);
    }
    
    @Test
    public void testGetReplacabeTypes()
    {
        int[] types = SqlUtils.getReplaceableTypes(Types.FLOAT);
        
        assertNotNull(types);
        
        assertTrue(types.length == 9);
        
        assertTrue(types[0] == Types.NUMERIC);
        assertTrue(types[1] == Types.DOUBLE);
        assertTrue(types[2] == Types.DECIMAL);
        assertTrue(types[3] == Types.REAL);
        assertTrue(types[4] == Types.BIGINT);
        assertTrue(types[5] == Types.INTEGER);
        assertTrue(types[6] == Types.SMALLINT);
        assertTrue(types[7] == Types.TINYINT);
        assertTrue(types[8] == Types.BIT);
    }
    
    @Test
    public void testGetScaleAndPrecision()
    {
        TypedKeyValue<Integer, Integer> value = SqlUtils
                .getScaleAndPrecision("VARCHAR(128)");
        
        assertNotNull(value);
        
        assertTrue(128 == value.getKey());
        assertTrue(value.getValue() == null);
        
        value = SqlUtils.getScaleAndPrecision("NUMBER(12,  23)");
        
        assertNotNull(value);
        
        assertTrue(12 == value.getKey());
        assertTrue(23 == value.getValue());
        
        value = SqlUtils.getScaleAndPrecision("NUMBER");
        
        assertNull(value);
        
        value = SqlUtils.getScaleAndPrecision("NUMBER()");
        
        assertNull(value);
        
        value = SqlUtils.getScaleAndPrecision(" ");
        
        assertNull(value);
        
        value = SqlUtils.getScaleAndPrecision(null);
        
        assertNull(value);
        
    }
    
    @Test
    public void testGetSelectSql()
    {
        assertTrue("select * from abc".equals(SqlUtils.getSelectSql("abc")));
        
        assertTrue("select * from 123".equals(SqlUtils
                .getSelectSql("select * from 123")));
        
        assertNull(SqlUtils.getSelectSql(null));
    }
    
    @Test
    public void testGetSqlForExplainPlan()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertTrue(SqlUtils.getSqlForExplainPlan(null, driver, null) == null);
        assertTrue(SqlUtils.getSqlForExplainPlan("", driver, null).equals(""));
        assertTrue(SqlUtils.getSqlForExplainPlan(" ", driver, null).equals(" "));
        
        assertTrue(SqlUtils.getSqlForExplainPlan("select * from dual", driver,
                null).equals("select * from dual"));
        
        assertTrue(SqlUtils.getSqlForExplainPlan(
                "select * from dual where a>:test1 and b < :test2", driver,
                null).equals("select * from dual where a>null and b < null"));
        
    }
    
    @Test
    public void testGetTypeAndValue()
    {
        TypedKeyValue<Integer, Object> typeAndValue = SqlUtils.getTypeAndValue(
                "abc", null);
        assertTrue(typeAndValue.getKey() == Types.VARCHAR);
        assertTrue("abc".equals(typeAndValue.getValue()));
        
        typeAndValue = SqlUtils.getTypeAndValue("123", null);
        assertTrue(typeAndValue.getKey() == Types.INTEGER);
        assertTrue(typeAndValue.getValue() instanceof Integer);
        assertTrue(123 == (Integer)typeAndValue.getValue());
        
        typeAndValue = SqlUtils.getTypeAndValue("123.12", null);
        assertTrue(typeAndValue.getKey() == Types.FLOAT);
        assertTrue(typeAndValue.getValue() instanceof Float);
        assertTrue(123.12f == (Float)typeAndValue.getValue());
        
        java.util.Date date = Utils.str2Date("01/01/2001 01:01:01", null,
                "dd/MM/yyyy HH:mm:ss");
        typeAndValue = SqlUtils.getTypeAndValue("2001-01-01 01:01:01", null);
        assertTrue(typeAndValue.getKey() == Types.TIMESTAMP);
        assertTrue(typeAndValue.getValue() instanceof java.util.Date);
        assertTrue(date.equals(typeAndValue.getValue()));
        
        date = Utils.str2Date("01:01:01", null, "HH:mm:ss");
        typeAndValue = SqlUtils.getTypeAndValue("01:01:01", null);
        assertTrue(typeAndValue.getKey() == Types.TIME);
        assertTrue(typeAndValue.getValue() instanceof java.util.Date);
        assertTrue(date.equals(typeAndValue.getValue()));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.DATE_FORMAT_PROP, "dd/MM/yyyy");
        date = Utils.str2Date("01/01/2001", null, "dd/MM/yyyy");
        typeAndValue = SqlUtils.getTypeAndValue("01/01/2001", params);
        assertTrue(typeAndValue.getKey() == Types.DATE);
        assertTrue(typeAndValue.getValue() instanceof java.util.Date);
        assertTrue(date.equals(typeAndValue.getValue()));
        
        params = new HashMap<String, String>();
        params.put(SqlUtils.CHAR_SEPARATOR_PROP, "\"");
        
        typeAndValue = SqlUtils.getTypeAndValue("\"123\"", params);
        assertTrue(typeAndValue.getKey() == Types.VARCHAR);
        assertTrue("123".equals(typeAndValue.getValue()));
        
        typeAndValue = SqlUtils.getTypeAndValue("\"abcd   \"", params);
        assertTrue(typeAndValue.getKey() == Types.VARCHAR);
        assertTrue("abcd   ".equals(typeAndValue.getValue()));
        
    }
    
    @Test
    public void testGetTypeByName()
    {
        assertNull(SqlUtils.getTypeByName(null));
        
        assertNull(SqlUtils.getTypeByName("sddsffds"));
        
        assertTrue(Types.CLOB == SqlUtils.getTypeByName("CLOB"));
        
        assertTrue(Types.BLOB == SqlUtils.getTypeByName("BloB"));
    }
    
    @Test
    public void testGetTypeName()
    {
        assertTrue("Unknown".equals(SqlUtils.getTypeName(null)));
        
        assertTrue("Unknown".equals(SqlUtils.getTypeName("abc")));
        
        assertTrue("VARCHAR".equals(SqlUtils.getTypeName(Types.VARCHAR)));
        
        assertTrue("777777777".equals(SqlUtils.getTypeName(777777777)));
    }
    
    @Test
    public void testGetUnquotedText()
    {
        assertTrue("\"123\"".equals(SqlUtils.getUnquotedText(null, "\"123\"")));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.CHAR_SEPARATOR_PROP, "\"");
        
        assertTrue("123".equals(SqlUtils.getUnquotedText(params, "\"123\"")));
        
    }
    
    @Test
    public void testGetValue()
    {
        assertTrue("   ".equals(SqlUtils.getValue("   ", 1, ";")));
        assertTrue("456".equals(SqlUtils.getValue("123;456;789", 1, ";")));
    }
    
    @Test
    public void testGetVarType()
    {
        assertTrue(SqlUtils.getVarType(null) == Types.VARCHAR);
        assertTrue(SqlUtils.getVarType("123") == Types.INTEGER);
        assertTrue(SqlUtils.getVarType("abc") == Types.VARCHAR);
        
        assertTrue(SqlUtils.getVarType("01/01/2007") == Types.DATE);
        
        assertTrue(SqlUtils.getVarType("Sat Aug 12 13:30:00 GMT 1995") == Types.TIMESTAMP);
        
        assertTrue(SqlUtils.getVarType("Sat, 12 Aug 1995 13:30:00 GMT") == Types.TIMESTAMP);
        
        assertTrue(SqlUtils.getVarType("Sat, 12 Aug 1995") == Types.DATE);
        
        assertTrue(SqlUtils.getVarType("Sat Aug 12 1995") == Types.DATE);
        
        assertTrue(SqlUtils.getVarType("01-01-2007") == Types.DATE);
        
        assertTrue(SqlUtils.getVarType("23-03-2007") == Types.DATE);
    }
    
    @Test
    public void testGetVarTypeByObjectType()
    {
        assertTrue(SqlUtils.getVarTypeByObjType("123") == Types.VARCHAR);
        assertTrue(SqlUtils.getVarTypeByObjType(123) == Types.NUMERIC);
        assertTrue(SqlUtils.getVarTypeByObjType(new java.util.Date()) == Types.DATE);
    }
    
    @Test
    public void testGetVisibleFieldsSql()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockCallableDriver", null, null);
        
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        
        dataSet.getFieldDef("datetimefield").setVisible(false);
        
        String sql = SqlUtils.getFieldsSql(dataSet, driver, null, null);
        
        assertTrue("numfield  INT ,\nstrfield  VARCHAR(8000) ,\nintfield  INT ,\nfloatfield  FLOAT ,\ndatefield  DATETIME ,\ntimefield  DATETIME \n"
                .equals(sql));
        
        dataSet.getFieldDef("datetimefield").setVisible(true);
        
        dataSet.getFieldDef("timefield").setVisible(false);
        
        sql = SqlUtils.getFieldsSql(dataSet, driver, null, null);
        
        assertTrue("numfield  INT ,\nstrfield  VARCHAR(8000) ,\ndatetimefield  DATETIME ,\nintfield  INT ,\nfloatfield  FLOAT ,\ndatefield  DATETIME \n"
                .equals(sql));
    }
    
    @Test
    public void testHasSize()
    {
        assertTrue(SqlUtils.dataTypeHasSize(Types.NUMERIC));
        assertTrue(SqlUtils.dataTypeHasSize(Types.VARCHAR));
        assertTrue(!SqlUtils.dataTypeHasSize(Types.INTEGER));
    }
    
    @Test
    public void testIsCompatible()
    {
        assertTrue(SqlUtils.isCompatible(Types.FLOAT, Types.FLOAT));
        assertTrue(SqlUtils.isCompatible(Types.FLOAT, Types.INTEGER));
        assertTrue(!SqlUtils.isCompatible(Types.INTEGER, Types.FLOAT));
        assertTrue(!SqlUtils.isCompatible(Types.BLOB, Types.CLOB));
        assertTrue(SqlUtils.isCompatible(Types.CHAR, Types.VARCHAR));
        assertTrue(SqlUtils.isCompatible(Types.NUMERIC, Types.INTEGER));
        assertTrue(!SqlUtils.isCompatible(Types.INTEGER, Types.NUMERIC));
    }
    
    @Test
    public void testLargeTypeCheck()
    {
        assertTrue(SqlUtils.isLargeObject(Types.LONGVARCHAR));
        assertTrue(!SqlUtils.isLargeObject(Types.VARCHAR));
        
        assertTrue(SqlUtils.isLargeObject(Types.BLOB));
        assertTrue(SqlUtils.isLargeObject(Types.CLOB));
    }
    
    @Test
    public void testName2RightCase()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockCallableDriver", null, null);
        
        assertTrue("teST".equals(SqlUtils.name2RightCase(driver, "teST")));
        
        driver = etlFactory
                .getDriver(
                        "com.toolsverse.etl.driver.MockExtendedCallableDriver::::::::lower::",
                        null, null);
        
        assertTrue("test".equals(SqlUtils.name2RightCase(driver, "teST")));
    }
    
    @Test
    public void testNumberTypeCheck()
    {
        assertTrue(!SqlUtils.isNumber(Types.LONGVARCHAR));
        
        assertTrue(SqlUtils.isNumber(Types.INTEGER));
        
        assertTrue(SqlUtils.isNumber(Types.NUMERIC));
    }
    
    @Test
    public void testOtherTypeCheck()
    {
        assertTrue(SqlUtils.isOther(Types.NULL));
        
        assertTrue(!SqlUtils.isOther(Types.VARCHAR));
    }
    
    @Test
    public void testParseSql()
    {
        assertTrue(SqlUtils.parseSql(null) == null);
        
        assertTrue(SqlUtils.parseSql("   ") == null);
        
        assertTrue(SqlUtils.parseSql("") == null);
        
        String[] stmts = SqlUtils.parseSql("select * from source");
        assertTrue(stmts != null);
        assertTrue(stmts.length == 1);
        assertTrue(stmts[0].equals("select * from source"));
        
        stmts = SqlUtils.parseSql("select * from source;select * from temp");
        assertTrue(stmts != null);
        assertTrue(stmts.length == 2);
        assertTrue(stmts[0].equals("select * from source"));
        assertTrue(stmts[1].equals("select * from temp"));
        
        stmts = SqlUtils.parseSql("select * from source;\nselect * from temp");
        assertTrue(stmts != null);
        assertTrue(stmts.length == 2);
        assertTrue(stmts[0].equals("select * from source"));
        assertTrue(stmts[1].equals("\nselect * from temp"));
    }
    
    @Test
    public void testPopulateDataSet()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        AliasConnectionProvider connectionProvider = new AliasConnectionProvider();
        
        Connection con = null;
        PreparedStatement st = null;
        
        DataSet sourceDataSet = new DataSet();
        sourceDataSet.setName("source");
        
        try
        {
            con = connectionProvider.getConnection(alias
                    .getConnectionParams());
            
            st = con.prepareStatement("select * from source");
            
            SqlUtils.populateDataSet(sourceDataSet, driver, st.executeQuery(),
                    null, false, false, null, null, -1);
            
            assertTrue(sourceDataSet.getFieldCount() > 0);
            assertTrue(sourceDataSet.getRecordCount() > 0);
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            connectionProvider.releaseConnection(con);
        }
        
    }
    
    @Test
    public void testPrepareParams()
    {
        String params = "";
        
        params = SqlUtils.prepareParams(params, 123);
        
        assertTrue("123".equals(params));
        
        params = SqlUtils.prepareParams(params, "abc");
        
        assertTrue("123,'abc'".equals(params));
    }
    
    @Test
    public void testRequiresTranslation()
    {
        assertTrue(SqlUtils.requiresTranslation(Types.TIMESTAMP));
        
        assertTrue(SqlUtils.requiresTranslation(Types.DATE));
        
        assertTrue(!SqlUtils.requiresTranslation(Types.VARCHAR));
        
        assertTrue(SqlUtils.requiresTranslation(Types.FLOAT));
        
        assertTrue(!SqlUtils.requiresTranslation(Types.INTEGER));
        
        assertTrue(SqlUtils.requiresTranslation(Types.DOUBLE));
        
        assertTrue(SqlUtils.requiresTranslation(Types.NUMERIC));
        
        assertTrue(SqlUtils.requiresTranslation(Types.DECIMAL));
        
        assertTrue(SqlUtils.requiresTranslation(Types.TIME));
        
        assertTrue(SqlUtils.requiresTranslation(Types.TIMESTAMP));
        
        assertTrue(!SqlUtils.requiresTranslation(Types.BLOB));
    }
    
    @Test
    public void testResultSet2Text()
        throws Exception
    {
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        AliasConnectionProvider connectionProvider = new AliasConnectionProvider();
        
        Connection con = null;
        PreparedStatement st = null;
        
        try
        {
            con = connectionProvider.getConnection(alias
                    .getConnectionParams());
            
            st = con.prepareStatement("select * from source");
            
            String result = SqlUtils.resultSet2Text(driver, st.executeQuery(),
                    null);
            
            assertTrue(!Utils.isNothing(result));
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            connectionProvider.releaseConnection(con);
        }
        
    }
    
    @Test
    public void testStorageValue2Value()
    {
        assertTrue(new Integer(123).equals(SqlUtils.storageValue2Value(
                Types.INTEGER, "123", null)));
        
        assertTrue("abc".equals(SqlUtils.storageValue2Value(Types.VARCHAR,
                "abc", null)));
        
        assertTrue(Utils.str2Date("2001-01-01 01:01:01", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT).equals(
                SqlUtils.storageValue2Value(Types.DATE, "2001-01-01 01:01:01",
                        null)));
    }
    
    @Test
    public void testStr2Date()
    {
        java.util.Date date = Utils.str2Date("02/02/2002 00:00:00", null,
                "dd/MM/yyyy HH:mm:ss");
        assertTrue(date.equals(SqlUtils.str2Date("2002-02-02 00:00:00", null)));
        
        date = Utils.str2Date("02/02/2002 02:02:02", null,
                "dd/MM/yyyy HH:mm:ss");
        assertTrue(date.equals(SqlUtils.str2DateTime("2002-02-02 02:02:02",
                null)));
        
        date = Utils.str2Date("02:02:02", null, "HH:mm:ss");
        assertTrue(date.equals(SqlUtils.str2Time("02:02:02", null)));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.DATE_TIME_FORMAT_PROP, "dd/MM/yyyy HH:mm:ss");
        
        date = Utils.str2Date("02/02/2002 02:02:02", null,
                "dd/MM/yyyy HH:mm:ss");
        assertTrue(date.equals(SqlUtils.str2DateTime("02/02/2002 02:02:02",
                params)));
    }
    
    @Test
    public void testTimeOnlyTypeCheck()
    {
        assertTrue(!SqlUtils.isTime(Types.DATE));
        
        assertTrue(SqlUtils.isTime(Types.TIME));
        
        assertTrue(!SqlUtils.isTime(Types.TIMESTAMP));
        
        assertTrue(!SqlUtils.isTime(Types.VARCHAR));
    }
    
    @Test
    public void testTimestampTypeCheck()
    {
        assertTrue(SqlUtils.isTimestamp(Types.TIMESTAMP));
        
        assertTrue(!SqlUtils.isTimestamp(Types.TIME));
        
        assertTrue(!SqlUtils.isTimestamp(Types.DATE));
        
        assertTrue(!SqlUtils.isTimestamp(Types.VARCHAR));
    }
    
    @Test
    public void testTypeRange()
    {
        assertTrue("(10,20)".equals(SqlUtils.getTypeRange("number(100, 200)",
                10, 20)));
        
        assertTrue("(10,10)".equals(SqlUtils.getTypeRange("number(100, 200)",
                10, 20, true)));
        
        assertTrue("(100,100)".equals(SqlUtils.getTypeRange("number(100, 200)",
                300, 300, true)));
        
        assertTrue("(100,200)".equals(SqlUtils.getTypeRange("number(100, 200)",
                300, 300, false)));
        
        assertTrue("(10)".equals(SqlUtils.getTypeRange("char(100)", 10, 0)));
        assertTrue("".equals(SqlUtils.getTypeRange("clob", 0, 0)));
        
        assertTrue("(100)".equals(SqlUtils.getTypeRange(
                "VARCHAR () FOR BIT DATA", 100, 0)));
        
        assertTrue("(100,50)".equals(SqlUtils.getTypeRange("SOMETHING () DATA",
                100, 50)));
        
        assertTrue("(10,20)".equals(SqlUtils.getTypeRange(
                "SOMETHING (1000,7000) DATA", 10, 20)));
        
        assertTrue("(45)".equals(SqlUtils.getTypeRange("SOMETHING (7000) DATA",
                45, 0)));
        
        assertTrue("(37)".equals(SqlUtils.getTypeRange("SOMETHING (max) DATA",
                37, 0)));
        
        assertTrue("".equals(SqlUtils
                .getTypeRange("SOMETHING (max) DATA", 0, 0)));
    }
    
    @Test
    public void testValue2DisplayValue()
    {
        assertTrue("123".equals(SqlUtils.value2DisplayValue(Types.INTEGER, 123,
                null)));
        assertTrue("abc".equals(SqlUtils.value2DisplayValue(Types.VARCHAR,
                "abc", null)));
        assertTrue("2001-01-01 01:01:01".equals(SqlUtils.value2DisplayValue(
                Types.DATE, Utils.str2Date("2001-01-01 01:01:01", null,
                        DataSet.DATA_SET_DATE_TIME_FORMAT), null)));
        
        assertTrue("123".equals(SqlUtils.value2DisplayValue(Types.DOUBLE,
                123.00, null)));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put(SqlUtils.DECIMAL_AS_INT_PROP, "false");
        
        assertTrue(String.valueOf(123.00).equals(
                SqlUtils.value2DisplayValue(Types.DOUBLE, 123.00, params)));
        
        assertTrue("test".equals(SqlUtils.value2DisplayValue(Types.CLOB,
                "test", null)));
        
        assertTrue("Blob".equals(SqlUtils.value2DisplayValue(Types.BLOB,
                "test", null)));
    }
    
    @Test
    public void testValue2StorageValue()
    {
        assertTrue("123".equals(SqlUtils.value2StorageValue(Types.INTEGER, 123,
                null)));
        assertTrue("abc".equals(SqlUtils.value2StorageValue(Types.VARCHAR,
                "abc", null)));
        assertTrue("2001-01-01 01:01:01".equals(SqlUtils.value2StorageValue(
                Types.DATE, Utils.str2Date("2001-01-01 01:01:01", null,
                        DataSet.DATA_SET_DATE_TIME_FORMAT), null)));
        
        assertTrue("123.0".equals(SqlUtils.value2StorageValue(Types.DOUBLE,
                123.0, null)));
    }
    
}