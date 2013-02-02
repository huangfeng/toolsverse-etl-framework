/*
 * DataSetTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * DataSetTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DataSetTest
{
    @BeforeClass
    public static void setUp()
        throws Exception
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
    }
    
    @Test
    public void testCellVersions()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        DataSetRecord record = dataSet.getRecord(1);
        
        assertTrue(record.getNumberOfVersions(1) == 1);
        
        record.addVersion(1, "abc");
        record.addVersion(1, "xyz");
        
        assertTrue(record.getNumberOfVersions(1) == 3);
        
        assertTrue(DataSetGenerator.FIELD2_RECORD2_VALUE.equals(record
                .getVersion(1, 0)));
        
        assertTrue("abc".equals(record.getVersion(1, 1)));
        assertTrue("xyz".equals(record.getVersion(1, 2)));
        
        record.removeVersion(1, 1);
        
        assertTrue(record.getNumberOfVersions(1) == 2);
        assertTrue("xyz".equals(record.getVersion(1, 1)));
        
        record.removeVersion(1, 2);
        
        assertTrue(record.getNumberOfVersions(1) == 2);
    }
    
    @Test
    public void testContains()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        DataSetRecord record = dataSet.getRecord(1);
        
        assertTrue(record.getNumberOfVersions(1) == 1);
        
        record.addVersion(1, "abc");
        record.addVersion(1, "xyz");
        
        assertTrue(record.getNumberOfVersions(1) == 3);
        
        assertTrue(!record.contains(1, null, true, true));
        
        assertTrue(record.contains(1, DataSetGenerator.FIELD2_RECORD2_VALUE,
                true, true));
        
        assertTrue(record.contains(1, "   aBc ", true, true));
        assertTrue(!record.contains(1, "   aBc ", false, true));
        assertTrue(record.contains(1, "aBc", true, false));
        assertTrue(record.contains(1, "xyz", true, true));
        assertTrue(!record.contains(1, "123", true, true));
    }
    
    @Test
    public void testCopyDataSet()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        dataSet.setName("test");
        dataSet.setTableName("table");
        assertNotNull(dataSet);
        
        DataSet dataSetCopy = dataSet.copy();
        
        assertTrue(dataSet.getName().equals(dataSetCopy.getName()));
        assertTrue(dataSet.getTableName().equals(dataSetCopy.getTableName()));
        
        assertTrue(dataSet.equals(dataSetCopy));
    }
    
    @Test
    public void testCopyField()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        FieldDef fieldDef = dataSet.getFieldDef(1);
        
        assertNotNull(dataSet);
        
        FieldDef copyField = fieldDef.copy();
        
        assertNotNull(copyField);
        
        assertTrue(copyField.getName().equals(fieldDef.getName()));
        assertTrue(copyField.getSqlDataType() == fieldDef.getSqlDataType());
        assertTrue(copyField.getNativeDataType().equals(
                fieldDef.getNativeDataType()));
        assertTrue(copyField.getFieldSize() == fieldDef.getFieldSize());
    }
    
    @Test
    public void testCreateDataSet()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        dataSet.setName("test");
        
        assertTrue(dataSet.getFieldCount() == DataSetGenerator.NUMBER_OF_FIELDS);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS);
        
        String dataLocation = SystemConfig.instance().getDataFolderName();
        
        assertTrue((dataLocation + dataSet.getName() + ".dat").equals(dataSet
                .getFileName(dataLocation, ".dat")));
    }
    
    @Test
    public void testDataSetCursor()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        DataSetRecord cursor = dataSet.getCursorRecord(driver);
        
        assertNotNull(cursor);
        assertTrue(cursor.size() == DataSetGenerator.NUMBER_OF_FIELDS);
    }
    
    @Test
    public void testDataSetEncode()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        dataSet.setEncode(true);
        
        String value = dataSet.encode(dataSet.getFieldDef(1),
                dataSet.getFieldValue(1, 1), driver, null);
        
        assertNotNull(value);
        assertTrue(DataSetGenerator.FIELD2_RECORD2_VALUE.equals(value));
        
        dataSet.getFieldDef(1).setSqlDataType(Types.CLOB);
        
        value = dataSet.encode(dataSet.getFieldDef(1),
                dataSet.getFieldValue(1, 1), driver, null);
        
        assertNotNull(value);
        assertTrue(!DataSetGenerator.FIELD2_RECORD2_VALUE.equals(value));
        
        value = (String)dataSet.decode(dataSet.getFieldDef(1), value, driver,
                null);
        assertTrue(DataSetGenerator.FIELD2_RECORD2_VALUE.equals(value));
    }
    
    @Test
    public void testDataSetEquals()
        throws Exception
    {
        DataSet dataSet1 = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet1);
        
        DataSet dataSet2 = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet2);
        
        DataSet dataSet3 = new DataSet();
        
        assertTrue(dataSet1.equals(dataSet2));
        
        assertTrue(!dataSet2.equals(dataSet3));
    }
    
    @Test
    public void testDataSetGetters()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getFieldCount() == DataSetGenerator.NUMBER_OF_FIELDS);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS);
        
        assertNotNull(dataSet.getFields());
        assertTrue(dataSet.getFields().size() == DataSetGenerator.NUMBER_OF_FIELDS);
        
        assertNotNull(dataSet.getData());
        assertTrue(dataSet.getData().size() == DataSetGenerator.NUMBER_OF_RECORDS);
        
        assertTrue(DataSetGenerator.FIELD1_NAME.equals(dataSet.getFieldDef(0)
                .getName()));
        assertTrue(DataSetGenerator.FIELD1_NATIVE_TYPE.equals(dataSet
                .getFieldDef(0).getNativeDataType()));
        assertTrue(DataSetGenerator.FIELD1_TYPE == dataSet.getFieldDef(0)
                .getSqlDataType());
        assertTrue(DataSetGenerator.FIELD1_SIZE == dataSet.getFieldDef(0)
                .getFieldSize());
        
        assertTrue(DataSetGenerator.FIELD2_NAME.equals(dataSet.getFieldDef(1)
                .getName()));
        assertTrue(DataSetGenerator.FIELD2_NATIVE_TYPE.equals(dataSet
                .getFieldDef(1).getNativeDataType()));
        assertTrue(DataSetGenerator.FIELD2_TYPE == dataSet.getFieldDef(1)
                .getSqlDataType());
        assertTrue(DataSetGenerator.FIELD2_SIZE == dataSet.getFieldDef(1)
                .getFieldSize());
        
        assertTrue(DataSetGenerator.FIELD3_NAME.equals(dataSet.getFieldDef(2)
                .getName()));
        assertTrue(DataSetGenerator.FIELD3_NATIVE_TYPE.equals(dataSet
                .getFieldDef(2).getNativeDataType()));
        assertTrue(DataSetGenerator.FIELD3_TYPE == dataSet.getFieldDef(2)
                .getSqlDataType());
        
        assertTrue(DataSetGenerator.FIELD3_NAME.equals(dataSet.getFieldDef(
                DataSetGenerator.FIELD3_NAME).getName()));
        
        assertTrue(dataSet.getFieldIndex(DataSetGenerator.FIELD2_NAME) == 1);
        
        DataSetRecord record = dataSet.getRecord(0);
        assertNotNull(record);
        assertTrue(record.size() == DataSetGenerator.NUMBER_OF_FIELDS);
        
        assertTrue(DataSetGenerator.FIELD1_RECORD1_VALUE.equals(record.get(0)));
        assertTrue(DataSetGenerator.FIELD2_RECORD1_VALUE.equals(record.get(1)));
        
        record = dataSet.getRecord(1);
        assertNotNull(record);
        assertTrue(record.size() == DataSetGenerator.NUMBER_OF_FIELDS);
        
        assertTrue(DataSetGenerator.FIELD1_RECORD2_VALUE.equals(record.get(0)));
        assertTrue(DataSetGenerator.FIELD2_RECORD2_VALUE.equals(record.get(1)));
        
        record = dataSet.getRecord(2);
        assertNotNull(record);
        assertTrue(record.size() == DataSetGenerator.NUMBER_OF_FIELDS);
        
        assertTrue(DataSetGenerator.FIELD1_RECORD3_VALUE.equals(record.get(0)));
        assertTrue(DataSetGenerator.FIELD2_RECORD3_VALUE.equals(record.get(1)));
        
        assertTrue(DataSetGenerator.FIELD1_RECORD1_VALUE.equals(dataSet
                .getFieldValue(dataSet.getRecord(0),
                        DataSetGenerator.FIELD1_NAME)));
        
        assertTrue(DataSetGenerator.FIELD1_RECORD1_VALUE.equals(dataSet
                .getFieldValue(dataSet.getRecord(0), 0)));
        
        assertTrue(DataSetGenerator.FIELD1_RECORD1_VALUE.equals(dataSet
                .getFieldValue(0, 0)));
        
    }
    
    @Test
    public void testDataSetKeys()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet(null,
                DataSetGenerator.FIELD1_NAME);
        assertNotNull(dataSet);
        
        assertNotNull(dataSet.getDataSetIndex());
        
        assertTrue(dataSet.getDataSetIndex().size() == dataSet.getRecordCount());
    }
    
    @Test
    public void testDataSetUpdate()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS);
        
        dataSet.deleteRecord(2);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS - 1);
        
        DataSetRecord record = DataSetGenerator.getRecord();
        assertNotNull(record);
        assertTrue(record.size() == DataSetGenerator.NUMBER_OF_FIELDS);
        
        assertTrue(DataSetGenerator.FIELD1_RECORD_VALUE.equals(record.get(0)));
        assertTrue(DataSetGenerator.FIELD2_RECORD_VALUE.equals(record.get(1)));
        
        dataSet.addRecord(record);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS);
        
        dataSet.addRecord(record, 2);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS + 1);
        
        dataSet.setRecord(record, 0);
        assertTrue(dataSet.getRecordCount() == DataSetGenerator.NUMBER_OF_RECORDS + 1);
        
        assertTrue(dataSet.getRecord(0) == record);
    }
    
    @Test
    public void testDenormalize()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        dataSet.setName("test");
        assertNotNull(dataSet);
        
        DataSet newDataSet = CommonEtlUtils.denormalize(dataSet, false);
        
        assertTrue(dataSet == newDataSet);
        
        dataSet.getFieldDef(1).addVersion();
        dataSet.getFieldDef(1).addVersion();
        dataSet.getFieldDef(1).addVersion();
        
        dataSet.getRecord(0).addVersion(1, "abc");
        dataSet.getRecord(0).addVersion(1, "xyz");
        
        dataSet.getRecord(1).addVersion(1, "123");
        
        dataSet.getRecord(2).addVersion(1, "aaa");
        dataSet.getRecord(2).addVersion(1, "bbb");
        dataSet.getRecord(2).addVersion(1, "ccc");
        
        dataSet.getRecord(3).addVersion(1, "mmm");
        dataSet.getRecord(3).addVersion(1, "nnn");
        
        newDataSet = CommonEtlUtils.denormalize(dataSet, false);
        
        assertTrue(dataSet.getFieldCount() > 0);
        assertTrue(dataSet.getRecordCount() > 0);
        
        assertTrue(dataSet != newDataSet);
        
        assertTrue(newDataSet.getFieldCount() == dataSet.getFieldCount() + 3);
        
        assertNotNull(newDataSet.getFieldDef(DataSetGenerator.FIELD2_NAME));
        assertNotNull(newDataSet.getFieldDef(DataSetGenerator.FIELD2_NAME
                + "_2"));
        assertNotNull(newDataSet.getFieldDef(DataSetGenerator.FIELD2_NAME
                + "_3"));
        assertNotNull(newDataSet.getFieldDef(DataSetGenerator.FIELD2_NAME
                + "_4"));
        
        assertTrue((DataSetGenerator.FIELD2_NAME + "_2").equals(newDataSet
                .getFieldDef(2).getName()));
        assertTrue((DataSetGenerator.FIELD2_NAME + "_3").equals(newDataSet
                .getFieldDef(3).getName()));
        assertTrue((DataSetGenerator.FIELD2_NAME + "_4").equals(newDataSet
                .getFieldDef(4).getName()));
        
        assertTrue(newDataSet.getRecord(0).get(1)
                .equals(DataSetGenerator.FIELD2_RECORD1_VALUE));
        assertTrue(newDataSet.getRecord(0).get(2).equals("abc"));
        assertTrue(newDataSet.getRecord(0).get(3).equals("xyz"));
        assertTrue(newDataSet.getRecord(0).get(4) == null);
        
        assertTrue(newDataSet.getRecord(1).get(1)
                .equals(DataSetGenerator.FIELD2_RECORD2_VALUE));
        assertTrue(newDataSet.getRecord(1).get(2).equals("123"));
        assertTrue(newDataSet.getRecord(1).get(3) == null);
        assertTrue(newDataSet.getRecord(1).get(4) == null);
        
        assertTrue(newDataSet.getRecord(2).get(1)
                .equals(DataSetGenerator.FIELD2_RECORD3_VALUE));
        assertTrue(newDataSet.getRecord(2).get(2).equals("aaa"));
        assertTrue(newDataSet.getRecord(2).get(3).equals("bbb"));
        assertTrue(newDataSet.getRecord(2).get(4).equals("ccc"));
        
        assertTrue(newDataSet.getRecord(3).get(1) == null);
        assertTrue(newDataSet.getRecord(3).get(2).equals("mmm"));
        assertTrue(newDataSet.getRecord(3).get(3).equals("nnn"));
        assertTrue(newDataSet.getRecord(3).get(4) == null);
        
        assertTrue(newDataSet.getRecord(4).get(1)
                .equals(DataSetGenerator.FIELD2_RECORD5_VALUE));
        assertTrue(newDataSet.getRecord(4).get(2) == null);
        assertTrue(newDataSet.getRecord(4).get(3) == null);
        assertTrue(newDataSet.getRecord(4).get(4) == null);
        
        newDataSet = CommonEtlUtils.denormalize(dataSet, true);
        
        assertTrue(dataSet.getFieldCount() == 0);
        assertTrue(dataSet.getRecordCount() == 0);
        
        assertTrue(dataSet != newDataSet);
    }
    
    @Test
    public void testFieldEncode()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        dataSet.setFieldAttr(dataSet.getFieldDef(1), DataSet.ENCODE_ATTR,
                "true");
        
        assertTrue(dataSet.isFieldEncoded(dataSet.getFieldDef(1)));
        
        String value = dataSet.encode(dataSet.getFieldDef(1), "abc", driver,
                null, true);
        
        assertTrue(!"abc".equals(value));
        
        assertTrue("abc".equals(dataSet.decode(dataSet.getFieldDef(1), value,
                driver, null)));
        
        dataSet.setFieldAttr(dataSet.getFieldDef(1), DataSet.ENCODE_ATTR,
                "false");
        
        assertTrue(!dataSet.isFieldEncoded(dataSet.getFieldDef(1)));
        
        value = dataSet.encode(dataSet.getFieldDef(1), "abc", driver, null,
                true);
        
        assertTrue("abc".equals(value));
        
        dataSet.setFieldAttr(dataSet.getFieldDef(1), DataSet.ENCODE_ATTR,
                "true");
        
        assertTrue(dataSet.isFieldEncoded(dataSet.getFieldDef(1)));
        
        value = dataSet.encode(dataSet.getFieldDef(1), "abc", driver, null,
                true);
        
        assertTrue(!"abc".equals(value));
        
        assertTrue("abc".equals(dataSet.decode(dataSet.getFieldDef(1), value,
                driver, null)));
        
    }
    
    @Test
    public void testFieldMarkedToDelete()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        dataSet.getFieldDef(2).setToDelete(true);
        dataSet.getFieldDef(6).setToDelete(true);
        
        DataSet newDataSet = CommonEtlUtils.denormalize(dataSet, false);
        
        assertTrue(newDataSet != dataSet);
        
        assertTrue(newDataSet.getFieldCount() == dataSet.getFieldCount() - 2);
        
        DataSet dataSet2 = DataSetGenerator.getTestDataSet2();
        
        assertTrue(dataSet2.equals(newDataSet));
    }
    
    @Test
    public void testFieldVersions()
    {
        FieldDef field3 = new FieldDef();
        
        assertNotNull(field3);
        
        assertTrue(field3.getVersions() == 1);
        
        field3.addVersion();
        field3.addVersion();
        
        assertTrue(field3.getVersions() == 3);
        
        field3.removeVersion();
        assertTrue(field3.getVersions() == 2);
        
        field3.removeVersion();
        field3.removeVersion();
        field3.removeVersion();
        assertTrue(field3.getVersions() == 1);
    }
    
    @Test
    public void testFilter()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator
                .getTestDataSet(DataSetGenerator.FIELD1_NAME + ">=2 and "
                        + DataSetGenerator.FIELD1_NAME + "<= 5 and "
                        + DataSetGenerator.FIELD1_NAME + "<> 3");
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 3);
        
        assertTrue(Utils.equals(2, dataSet.getFieldValue(0, 0)));
        assertTrue(Utils.equals(4, dataSet.getFieldValue(1, 0)));
        assertTrue(Utils.equals(5, dataSet.getFieldValue(2, 0)));
    }
    
    @Test
    public void testGetFieldAttr()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        dataSet.setFieldAttr(dataSet.getFieldDef(1), "test", "abc");
        
        assertTrue("abc".equals(dataSet.getFieldAttr(dataSet.getFieldDef(1),
                "test")));
    }
    
    @Test
    public void testGetNonCaseSensitiveFields()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        TreeMap<String, FieldDef> fields = dataSet.getNonCaseSensitiveFields();
        
        assertTrue(fields.size() == dataSet.getFieldCount());
        
        FieldDef field = fields.get(DataSetGenerator.FIELD1_NAME);
        assertNotNull(field);
        
        field = fields.get(DataSetGenerator.FIELD1_NAME.toUpperCase());
        assertNotNull(field);
        
        field = fields.get(DataSetGenerator.FIELD1_NAME.toLowerCase());
        assertNotNull(field);
    }
    
    @Test
    public void testGetSelectedDataSet()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        assertTrue(dataSet == CommonEtlUtils.getSelectedDataSet(dataSet, null));
        
        assertTrue(null == CommonEtlUtils.getSelectedDataSet(null, null));
        
        int[] rows = {1, 2, 3};
        int[] cols = {1, 2};
        
        TypedKeyValue<int[], int[]> selected = new TypedKeyValue<int[], int[]>(
                rows, cols);
        
        DataSet newDataSet = CommonEtlUtils.getSelectedDataSet(dataSet,
                selected);
        assertNotNull(newDataSet);
        
        assertTrue(newDataSet.getRecordCount() == 3);
        assertTrue(newDataSet.getFieldCount() == 2);
    }
    
    @Test
    public void testIsFieldEncoded()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        dataSet.setFieldAttr(dataSet.getFieldDef(1), DataSet.ENCODE_ATTR,
                "true");
        assertTrue(dataSet.isFieldEncoded(dataSet.getFieldDef(1)));
        
        assertTrue(!dataSet.isFieldEncoded(dataSet.getFieldDef(0)));
    }
    
    @Test
    public void testJoin()
        throws Exception
    {
        DataSet dataSet1 = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet1);
        
        DataSet dataSet2 = new DataSet();
        
        assertNull(CommonEtlUtils.join(null, null, null, false, null, null));
        
        assertNull(CommonEtlUtils.join(null, dataSet2, null, false, null, null));
        
        assertTrue(CommonEtlUtils.join(dataSet1, null, null, false, null, null) == dataSet1);
        
        assertTrue(CommonEtlUtils.join(dataSet1, dataSet2, null, false, null,
                null) == dataSet1);
        
        dataSet2 = DataSetGenerator.getTestDataSet3(null);
        assertNotNull(dataSet2);
        
        DataSet dataSet = CommonEtlUtils.join(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false, null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 3);
        
        assertTrue(dataSet.getFieldCount() == dataSet1.getFieldCount()
                + dataSet2.getFieldCount() - 1);
        
        dataSet2 = DataSetGenerator
                .getTestDataSet3(DataSetGenerator.FIELD1_NAME);
        assertNotNull(dataSet2);
        
        dataSet = CommonEtlUtils.join(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false, null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 3);
        
        assertTrue(dataSet.getFieldCount() == dataSet1.getFieldCount()
                + dataSet2.getFieldCount() - 1);
        
        dataSet = CommonEtlUtils.join(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, true, null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == dataSet1.getRecordCount());
        
        assertTrue(dataSet.getFieldCount() == dataSet1.getFieldCount()
                + dataSet2.getFieldCount() - 1);
        
        dataSet2 = DataSetGenerator
                .getTestDataSet3(DataSetGenerator.FIELD1_NAME);
        assertNotNull(dataSet2);
        
        dataSet = CommonEtlUtils.join(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false,
                DataSetGenerator.FIELD1_NAME + ","
                        + DataSetGenerator.FIELD8_NAME, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 3);
        
        assertTrue(dataSet.getFieldCount() == 2);
        
        dataSet = CommonEtlUtils.join(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false, null,
                DataSetGenerator.FIELD3_NAME + ","
                        + DataSetGenerator.FIELD8_NAME);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == 3);
        
        assertTrue(dataSet.getFieldCount() == dataSet1.getFieldCount() + -1);
    }
    
    @Test
    public void testMinus()
        throws Exception
    {
        DataSet drivingDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(drivingDataSet);
        
        DataSet minusDataSet = DataSetGenerator
                .getTestDataSet3(DataSetGenerator.FIELD1_NAME);
        assertNotNull(minusDataSet);
        
        DataSet dataSet = CommonEtlUtils.minus(drivingDataSet, minusDataSet,
                DataSetGenerator.FIELD1_NAME);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == drivingDataSet.getRecordCount()
                - minusDataSet.getRecordCount());
    }
    
    @Test
    public void testSortBy()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet);
        
        int recordCount = dataSet.getRecordCount();
        
        dataSet.sortByString(DataSetGenerator.FIELD1_NAME + " desc");
        
        assertTrue(recordCount == dataSet.getRecordCount());
        
        assertTrue(dataSet.getFieldValue(0, 0).equals(
                DataSetGenerator.FIELD1_RECORD5_VALUE));
        
        assertTrue(dataSet.getFieldValue(recordCount - 1, 0).equals(
                DataSetGenerator.FIELD1_RECORD1_VALUE));
        
        dataSet.sortByString(DataSetGenerator.FIELD1_NAME);
        
        assertTrue(recordCount == dataSet.getRecordCount());
        
        assertTrue(dataSet.getFieldValue(0, 0).equals(
                DataSetGenerator.FIELD1_RECORD1_VALUE));
        
        assertTrue(dataSet.getFieldValue(recordCount - 1, 0).equals(
                DataSetGenerator.FIELD1_RECORD5_VALUE));
        
    }
    
    @Test
    public void testSplit()
        throws Exception
    {
        DataSet dataSet = DataSetGenerator.getTestDataSet4();
        assertNotNull(dataSet);
        
        assertNull(CommonEtlUtils.split(null, null));
        
        LinkedHashMap<String, DataSet> map = CommonEtlUtils
                .split(dataSet, null);
        
        assertNotNull(map);
        assertTrue(map.size() == 1);
        assertTrue(map.containsKey(LinkedHashMap.class.getName()));
        
        map = CommonEtlUtils.split(dataSet, "dsadsadsa");
        
        assertNotNull(map);
        assertTrue(map.size() == 1);
        assertTrue(map.containsKey(LinkedHashMap.class.getName()));
        
        map = CommonEtlUtils.split(dataSet, DataSetGenerator.FIELD1_NAME);
        
        assertNotNull(map);
        assertTrue(map.size() == 3);
        
        DataSet ds = map.get(DataSetGenerator.FIELD1_RECORD1_VALUE.toString());
        assertNotNull(ds);
        assertTrue(ds.getRecordCount() == 1);
        
        ds = map.get(DataSetGenerator.FIELD1_RECORD2_VALUE.toString());
        assertNotNull(ds);
        assertTrue(ds.getRecordCount() == 3);
        
        ds = map.get(DataSetGenerator.FIELD1_RECORD3_VALUE.toString());
        assertNotNull(ds);
        assertTrue(ds.getRecordCount() == 1);
    }
    
    @Test
    public void testUnion()
        throws Exception
    {
        DataSet dataSet1 = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet1);
        
        DataSet dataSet2 = DataSetGenerator.getTestDataSet();
        assertNotNull(dataSet2);
        
        assertNull(CommonEtlUtils.union(null, null, null, true, null, null));
        
        assertTrue(CommonEtlUtils.union(dataSet1, null, null, true, null, null) == dataSet1);
        
        assertTrue(CommonEtlUtils.union(null, dataSet2, null, true, null, null) == dataSet2);
        
        DataSet dataSet = CommonEtlUtils.union(dataSet1, dataSet2, null, true,
                null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == dataSet1.getRecordCount()
                + dataSet2.getRecordCount());
        
        dataSet = CommonEtlUtils.union(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false, null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == dataSet1.getRecordCount());
        
        dataSet = CommonEtlUtils.union(dataSet1, dataSet2, null, true,
                DataSetGenerator.FIELD1_NAME + ","
                        + DataSetGenerator.FIELD2_NAME, null);
        
        assertNotNull(dataSet);
        
        // assertTrue(false);
        
        assertTrue(dataSet.getFieldCount() == 2);
        
        dataSet = CommonEtlUtils.union(dataSet1, dataSet2, null, true, null,
                DataSetGenerator.FIELD1_NAME + ","
                        + DataSetGenerator.FIELD2_NAME);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getFieldCount() == dataSet1.getFieldCount() - 2);
        
        dataSet1 = DataSetGenerator.getTestDataSet(null,
                DataSetGenerator.FIELD1_NAME);
        assertNotNull(dataSet1);
        
        dataSet = CommonEtlUtils.union(dataSet1, dataSet2,
                DataSetGenerator.FIELD1_NAME, false, null, null);
        
        assertNotNull(dataSet);
        
        assertTrue(dataSet.getRecordCount() == dataSet1.getRecordCount());
    }
    
}