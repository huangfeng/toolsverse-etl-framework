/*
 * DataSetGenerator.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.sql.Types;

import com.toolsverse.util.Utils;

/**
 * DataSetGenerator
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DataSetGenerator
{
    public static final String FIELD1_NAME = "numfield";
    public static final String FIELD2_NAME = "strfield";
    public static final String FIELD3_NAME = "datetimefield";
    public static final String FIELD4_NAME = "intfield";
    public static final String FIELD5_NAME = "floatfield";
    public static final String FIELD6_NAME = "datefield";
    public static final String FIELD7_NAME = "timefield";
    
    public static final String FIELD8_NAME = "col2";
    
    public static final int NUMBER_OF_FIELDS = 7;
    
    public static final String FIELD1_NATIVE_TYPE = "NUMERIC";
    public static final String FIELD2_NATIVE_TYPE = "VARCHAR";
    public static final String FIELD3_NATIVE_TYPE = "DATETIME";
    public static final String FIELD4_NATIVE_TYPE = "NUMERIC";
    public static final String FIELD5_NATIVE_TYPE = "NUMERIC";
    public static final String FIELD6_NATIVE_TYPE = "DATE";
    public static final String FIELD7_NATIVE_TYPE = "TIME";
    
    public static final int FIELD1_TYPE = Types.INTEGER;
    public static final int FIELD2_TYPE = Types.VARCHAR;
    public static final int FIELD3_TYPE = Types.TIMESTAMP;
    public static final int FIELD4_TYPE = Types.INTEGER;
    public static final int FIELD5_TYPE = Types.FLOAT;
    public static final int FIELD6_TYPE = Types.DATE;
    public static final int FIELD7_TYPE = Types.TIME;
    
    public static final int FIELD1_SIZE = 18;
    public static final int FIELD2_SIZE = 100;
    
    public static final int NUMBER_OF_RECORDS = 5;
    
    public static final Integer FIELD1_RECORD1_VALUE = 1;
    public static final String FIELD2_RECORD1_VALUE = "record1";
    public static final Integer FIELD4_RECORD1_VALUE = 123;
    public static final Float FIELD5_RECORD1_VALUE = 123.01f;
    
    public static final Integer FIELD1_RECORD2_VALUE = 2;
    public static final String FIELD2_RECORD2_VALUE = "record2";
    public static final Integer FIELD4_RECORD2_VALUE = 456;
    public static final Float FIELD5_RECORD2_VALUE = 456.02f;
    
    public static final Integer FIELD1_RECORD3_VALUE = 3;
    public static final String FIELD2_RECORD3_VALUE = "record3";
    public static final Integer FIELD4_RECORD3_VALUE = 789;
    public static final Float FIELD5_RECORD3_VALUE = 789.02f;
    
    public static final Integer FIELD1_RECORD4_VALUE = 4;
    public static final String FIELD2_RECORD4_VALUE = null;
    public static final Integer FIELD4_RECORD4_VALUE = null;
    public static final Float FIELD5_RECORD4_VALUE = null;
    
    public static final Integer FIELD1_RECORD5_VALUE = 5;
    public static final String FIELD2_RECORD5_VALUE = "record5";
    public static final Integer FIELD4_RECORD5_VALUE = 111;
    public static final Float FIELD5_RECORD5_VALUE = 22.01f;
    
    public static final Integer FIELD1_RECORD_VALUE = 99;
    public static final String FIELD2_RECORD_VALUE = "record99";
    public static final Integer FIELD4_RECORD_VALUE = 777;
    public static final Float FIELD5_RECORD_VALUE = 777.02f;
    
    public static DataSetRecord getRecord()
    {
        DataSetRecord record = new DataSetRecord();
        record.add(FIELD1_RECORD_VALUE);
        record.add(FIELD2_RECORD_VALUE);
        record.add(null);
        record.add(FIELD4_RECORD_VALUE);
        record.add(FIELD5_RECORD_VALUE);
        record.add(null);
        record.add(null);
        
        return record;
    }
    
    public static DataSet getTestDataSet()
        throws Exception
    {
        return getTestDataSet(null);
    }
    
    public static DataSet getTestDataSet(String filter)
        throws Exception
    {
        return getTestDataSet(filter, null);
    }
    
    public static DataSet getTestDataSet(String filter, String keyField)
        throws Exception
    {
        DataSet dataSet = new DataSet();
        
        dataSet.setKeyFields(keyField);
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName(FIELD1_NAME);
        fieldDef.setSqlDataType(FIELD1_TYPE);
        fieldDef.setNativeDataType(FIELD1_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD1_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD2_NAME);
        fieldDef.setSqlDataType(FIELD2_TYPE);
        fieldDef.setNativeDataType(FIELD2_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD2_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD3_NAME);
        fieldDef.setSqlDataType(FIELD3_TYPE);
        fieldDef.setNativeDataType(FIELD3_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD4_NAME);
        fieldDef.setSqlDataType(FIELD4_TYPE);
        fieldDef.setNativeDataType(FIELD4_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD5_NAME);
        fieldDef.setSqlDataType(FIELD5_TYPE);
        fieldDef.setNativeDataType(FIELD5_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD6_NAME);
        fieldDef.setSqlDataType(FIELD6_TYPE);
        fieldDef.setNativeDataType(FIELD6_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD7_NAME);
        fieldDef.setSqlDataType(FIELD7_TYPE);
        fieldDef.setNativeDataType(FIELD7_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        dataSet.setFilter(filter);
        
        DataSetRecord record = new DataSetRecord();
        record.add(FIELD1_RECORD1_VALUE);
        record.add(FIELD2_RECORD1_VALUE);
        record.add(Utils.str2Date("2003-03-03 03:03:03", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(FIELD4_RECORD1_VALUE);
        record.add(FIELD5_RECORD1_VALUE);
        record.add(Utils.str2Date("2005-05-05 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(Utils.str2Date("09:09:09", null,
                DataSet.DATA_SET_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD2_VALUE);
        record.add(Utils.str2Date("2002-02-02 02:02:02", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(FIELD4_RECORD2_VALUE);
        record.add(FIELD5_RECORD2_VALUE);
        record.add(Utils.str2Date("2007-07-07 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(Utils.str2Date("10:10:10", null,
                DataSet.DATA_SET_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD3_VALUE);
        record.add(FIELD2_RECORD3_VALUE);
        record.add(Utils.str2Date("2001-01-01 01:01:01", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(FIELD4_RECORD3_VALUE);
        record.add(FIELD5_RECORD3_VALUE);
        record.add(Utils.str2Date("2008-08-08 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(Utils.str2Date("11:11:11", null,
                DataSet.DATA_SET_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD4_VALUE);
        record.add(FIELD2_RECORD4_VALUE);
        record.add(Utils.str2Date("2011-11-11 01:01:01", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(FIELD4_RECORD4_VALUE);
        record.add(FIELD5_RECORD4_VALUE);
        record.add(Utils.str2Date("2012-01-01 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        record.add(Utils.str2Date("02:02:02", null,
                DataSet.DATA_SET_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD5_VALUE);
        record.add(FIELD2_RECORD5_VALUE);
        record.add(null);
        record.add(FIELD4_RECORD5_VALUE);
        record.add(FIELD5_RECORD5_VALUE);
        record.add(null);
        record.add(null);
        dataSet.addRecord(record);
        
        return dataSet;
    }
    
    public static DataSet getTestDataSet2()
    {
        DataSet dataSet = new DataSet();
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName(FIELD1_NAME);
        fieldDef.setSqlDataType(FIELD1_TYPE);
        fieldDef.setNativeDataType(FIELD1_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD1_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD2_NAME);
        fieldDef.setSqlDataType(FIELD2_TYPE);
        fieldDef.setNativeDataType(FIELD2_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD2_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD4_NAME);
        fieldDef.setSqlDataType(FIELD4_TYPE);
        fieldDef.setNativeDataType(FIELD4_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD5_NAME);
        fieldDef.setSqlDataType(FIELD5_TYPE);
        fieldDef.setNativeDataType(FIELD5_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD6_NAME);
        fieldDef.setSqlDataType(FIELD6_TYPE);
        fieldDef.setNativeDataType(FIELD6_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        DataSetRecord record = new DataSetRecord();
        record.add(FIELD1_RECORD1_VALUE);
        record.add(FIELD2_RECORD1_VALUE);
        record.add(FIELD4_RECORD1_VALUE);
        record.add(FIELD5_RECORD1_VALUE);
        record.add(Utils.str2Date("2005-05-05 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD2_VALUE);
        record.add(FIELD4_RECORD2_VALUE);
        record.add(FIELD5_RECORD2_VALUE);
        record.add(Utils.str2Date("2007-07-07 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD3_VALUE);
        record.add(FIELD2_RECORD3_VALUE);
        record.add(FIELD4_RECORD3_VALUE);
        record.add(FIELD5_RECORD3_VALUE);
        record.add(Utils.str2Date("2008-08-08 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD4_VALUE);
        record.add(FIELD2_RECORD4_VALUE);
        record.add(FIELD4_RECORD4_VALUE);
        record.add(FIELD5_RECORD4_VALUE);
        record.add(Utils.str2Date("2012-01-01 00:00:00", null,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD5_VALUE);
        record.add(FIELD2_RECORD5_VALUE);
        record.add(FIELD4_RECORD5_VALUE);
        record.add(FIELD5_RECORD5_VALUE);
        record.add(null);
        dataSet.addRecord(record);
        
        return dataSet;
    }
    
    public static DataSet getTestDataSet3(String keyField)
    {
        DataSet dataSet = new DataSet();
        
        dataSet.setKeyFields(keyField);
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName(FIELD1_NAME);
        fieldDef.setSqlDataType(FIELD1_TYPE);
        fieldDef.setNativeDataType(FIELD1_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD1_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD8_NAME);
        fieldDef.setSqlDataType(FIELD2_TYPE);
        fieldDef.setNativeDataType(FIELD2_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        DataSetRecord record = new DataSetRecord();
        record.add(FIELD1_RECORD1_VALUE);
        record.add(FIELD2_RECORD1_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD2_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD3_VALUE);
        record.add(FIELD2_RECORD3_VALUE);
        dataSet.addRecord(record);
        
        return dataSet;
    }
    
    public static DataSet getTestDataSet4()
    {
        DataSet dataSet = new DataSet();
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName(FIELD1_NAME);
        fieldDef.setSqlDataType(FIELD1_TYPE);
        fieldDef.setNativeDataType(FIELD1_NATIVE_TYPE);
        fieldDef.setFieldSize(FIELD1_SIZE);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName(FIELD8_NAME);
        fieldDef.setSqlDataType(FIELD2_TYPE);
        fieldDef.setNativeDataType(FIELD2_NATIVE_TYPE);
        dataSet.addField(fieldDef);
        
        DataSetRecord record = new DataSetRecord();
        record.add(FIELD1_RECORD1_VALUE);
        record.add(FIELD2_RECORD1_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD2_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD3_VALUE);
        record.add(FIELD2_RECORD3_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD4_VALUE);
        dataSet.addRecord(record);
        
        record = new DataSetRecord();
        record.add(FIELD1_RECORD2_VALUE);
        record.add(FIELD2_RECORD5_VALUE);
        dataSet.addRecord(record);
        
        return dataSet;
    }
    
}