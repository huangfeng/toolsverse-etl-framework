/*
 * CommonEtlUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.awt.Point;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * The collection of static methods used by core and common ETL components.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public final class CommonEtlUtils
{
    
    /**
     * Pivot data set which has multiple versions of the same field.
     *
     * @param source the source data set
     * @param cleanUp is <code>true</code> the source data set will be cleared
     * @return denormalized data set
     */
    public static DataSet denormalize(DataSet source, boolean cleanUp)
    {
        int count = source.getFieldCount();
        
        if (count == 0)
            return source;
        
        ListHashMap<String, FieldDef> sourceFields = source.getFields();
        
        ListHashMap<String, FieldDef> fields = new ListHashMap<String, FieldDef>();
        
        Map<Integer, Point> fldMap = new HashMap<Integer, Point>();
        
        boolean toDelete = false;
        
        for (int col = 0; col < count; col++)
        {
            FieldDef field = sourceFields.get(col);
            
            toDelete = toDelete || field.isMarkedToDelete();
            
            if (!field.isMarkedToDelete())
            {
                fields.put(field.getName(), field);
                
                fldMap.put(fields.size() - 1, new Point(col, 0));
            }
            
            int versionsCount = field.getVersions();
            
            if (versionsCount == 1 || field.isMarkedToDelete())
                continue;
            
            for (int i = 0; i < versionsCount - 1; i++)
            {
                FieldDef fieldVersion = field.copy();
                
                fieldVersion.setName(field.getName() + "_"
                        + String.valueOf(i + 2));
                
                fields.put(fieldVersion.getName(), fieldVersion);
                
                fldMap.put(fields.size() - 1, new Point(col, i + 1));
            }
            
        }
        
        int fieldsCount = fields.size();
        
        if (fieldsCount == count && !toDelete)
            return source;
        
        DataSet dataSet = source.copy();
        dataSet.setFields(fields);
        
        DataSetData data = new DataSetData();
        dataSet.setData(data);
        
        int rows = source.getRecordCount();
        
        if (rows > 0)
            for (int row = 0; row < rows; row++)
            {
                DataSetRecord record = source.getRecord(row);
                
                DataSetRecord newRecord = new DataSetRecord();
                
                for (int col = 0; col < fieldsCount; col++)
                {
                    Point point = fldMap.get(col);
                    
                    Object value = record.getVersion((int)point.getX(),
                            (int)point.getY());
                    
                    newRecord.add(value);
                }
                
                if (cleanUp)
                {
                    source.setRecord(null, row);
                }
                
                data.add(newRecord);
            }
        
        if (cleanUp)
        {
            source.setFields(null);
            source.setData(null);
        }
        
        return dataSet;
    }
    
    /**
     * Find record in the data set by key.
     * 
     * @param key the key
     * @param dataSet the data set
     * @param useIndex if true use index
     * @param dataSetIndex the external index to build
     * @param keyFields the key fields to use together with dataSetIndex
     * @return the record
     */
    private static DataSetRecord findRecord(String key, DataSet dataSet,
            boolean useIndex, Map<String, DataSetRecord> dataSetIndex,
            Map<String, FieldDef> keyFields)
    {
        if (useIndex)
            return dataSet.getRecord(key);
        
        if (dataSetIndex.size() > 0)
            return dataSetIndex.get(key);
        
        int rows = dataSet.getRecordCount();
        
        if (rows == 0)
            return null;
        
        DataSetRecord foundRecord = null;
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = dataSet.getRecord(row);
            
            String recordKey = getKey(dataSet, record, keyFields, false, false);
            
            if (key.equals(recordKey))
                foundRecord = record;
            
            dataSetIndex.put(recordKey, record);
        }
        
        return foundRecord;
        
    }
    
    /**
     * Gets the map of fields after include and exclude.
     *
     * @param dataSet the data set
     * @param includeFields the include fields
     * @param excludeFields the exclude fields
     * @return the fields after include and exclude
     */
    public static ListHashMap<String, FieldDef> getFieldsAfterIncludeExclude(
            DataSet dataSet, Set<String> includeFields,
            Set<String> excludeFields)
    {
        boolean isSelective = (excludeFields != null && !excludeFields
                .isEmpty())
                || (includeFields != null && !includeFields.isEmpty());
        
        ListHashMap<String, FieldDef> fields = new ListHashMap<String, FieldDef>();
        
        for (int col = 0; col < dataSet.getFieldCount(); col++)
        {
            FieldDef field = dataSet.getFieldDef(col);
            
            if (isSelective)
            {
                if (includeFields != null && !includeFields.isEmpty()
                        && !includeFields.contains(field.getName()))
                    continue;
                
                if (excludeFields != null && !excludeFields.isEmpty()
                        && excludeFields.contains(field.getName()))
                    continue;
            }
            
            fields.put(field.getName(), field);
        }
        
        return fields;
    }
    
    /**
     * Gets the string representation of the key for the given record and map of key fields.
     *
     * @param dataSet the data set
     * @param record the record
     * @param keys the key fields
     * @param ignoreCase if <code>true</code> ignore char case
     * @param doTrim if <code>true</code> truncate string
     * @return the string representation of the key
     */
    public static String getKey(DataSet dataSet, DataSetRecord record,
            Map<String, FieldDef> keys, boolean ignoreCase, boolean doTrim)
    {
        String key = "";
        
        for (FieldDef field : keys.values())
        {
            Object value = dataSet.getFieldValue(record, field.getName());
            
            String fldValue = (value == null || (doTrim && Utils
                    .isNothing(value))) ? "null" : value.toString();
            
            fldValue = doTrim ? fldValue.trim() : fldValue;
            fldValue = ignoreCase ? fldValue.toUpperCase() : fldValue;
            
            key = key + fldValue;
        }
        
        return key;
    }
    
    /**
     * Returns the map of the key fields for the given keys.
     * @param keys the keys
     * @param dataSetFields the fields
     * @return the key fields
     */
    public static Map<String, FieldDef> getKeyFields(String keys,
            Map<String, FieldDef> dataSetFields)
    {
        Map<String, FieldDef> fields = new LinkedHashMap<String, FieldDef>();
        
        if (Utils.isNothing(keys))
        {
            return fields;
        }
        
        String[] tokens = keys.split(",", -1);
        
        for (String name : tokens)
        {
            FieldDef field = dataSetFields.get(name);
            
            if (field == null)
                return fields;
            
            fields.put(field.getName(), field);
        }
        
        return fields;
    }
    
    /**
     * Gets the record after inlcude and exclude.
     *
     * @param dataSet the data set
     * @param record the source record
     * @param cols the number of columns
     * @param isSelective if true select only fields which are included or not excluded
     * @param includeFields the include fields
     * @param excludeFields the exclude fields
     * @return the record after inlcude and exclude
     */
    public static DataSetRecord getRecordAfterInlcudeExclude(DataSet dataSet,
            DataSetRecord record, int cols, boolean isSelective,
            Set<String> includeFields, Set<String> excludeFields)
    {
        DataSetRecord recordToAdd = new DataSetRecord();
        
        for (int col = 0; col < cols; col++)
        {
            if (isSelective)
            {
                FieldDef field = dataSet.getFieldDef(col);
                
                if (includeFields != null && !includeFields.isEmpty()
                        && !includeFields.contains(field.getName()))
                    continue;
                
                if (excludeFields != null && !excludeFields.isEmpty()
                        && excludeFields.contains(field.getName()))
                    continue;
            }
            
            recordToAdd.add(record.get(col));
        }
        
        return recordToAdd;
    }
    
    /**
     * Gets the selected data set.
     *
     * @param dataSet the data set
     * @param selected the selected rows and columns
     * @return the selected data set
     */
    public static DataSet getSelectedDataSet(DataSet dataSet,
            TypedKeyValue<int[], int[]> selected)
    {
        if (selected == null || dataSet == null)
            return dataSet;
        
        DataSet newDataSet = dataSet.copy();
        
        newDataSet.clear();
        
        for (int col : selected.getValue())
        {
            newDataSet.addField(dataSet.getFieldDef(col));
        }
        
        for (int row : selected.getKey())
        {
            DataSetRecord record = new DataSetRecord();
            
            for (int col : selected.getValue())
            {
                record.add(dataSet.getRecord(row).get(col));
            }
            
            newDataSet.addRecord(record);
        }
        
        return newDataSet;
    }
    
    /**
     * Joins two data sets.
     * 
     * @param drivingDataSet the driving data set
     * @param dataSetToJoin the data set to join
     * @param keys the key fields
     * @param outer if true perform outer join
     * @param include the fields to include
     * @param exclude the fields to exclude
     * @return the data set
     * @throws Exception in case of any error
     */
    public static DataSet join(DataSet drivingDataSet, DataSet dataSetToJoin,
            String keys, boolean outer, String include, String exclude)
        throws Exception
    {
        if (drivingDataSet == null)
            return null;
        
        if (dataSetToJoin == null)
            return drivingDataSet;
        
        if (drivingDataSet.isEmpty() || dataSetToJoin.isEmpty())
            return drivingDataSet;
        
        if (Utils.isNothing(keys))
            keys = dataSetToJoin.getKeyFields();
        
        if (Utils.isNothing(keys))
            return drivingDataSet;
        
        Map<String, FieldDef> keyFields = getKeyFields(keys,
                drivingDataSet.getFields());
        
        if (keyFields.isEmpty())
            throw new Exception(
                    EtlResource.ERROR_JOINING_DATA_SET_WRONG_KEYS_IN_DRIVING
                            .getValue());
        
        keyFields = getKeyFields(keys, dataSetToJoin.getFields());
        
        if (keyFields.isEmpty())
            throw new Exception(
                    EtlResource.ERROR_JOINING_DATA_SET_WRONG_KEYS_IN_JOIN
                            .getValue());
        
        Set<String> excludeFields = Utils.setSplit(exclude, ",");
        Set<String> includeFields = Utils.setSplit(include, ",");
        
        boolean isSelective = (excludeFields != null && !excludeFields
                .isEmpty())
                || (includeFields != null && !includeFields.isEmpty());
        
        ListHashMap<String, FieldDef> fields = getFieldsAfterIncludeExclude(
                drivingDataSet, includeFields, excludeFields);
        
        List<Integer> fieldsToAdd = new ArrayList<Integer>();
        
        for (int col = 0; col < dataSetToJoin.getFieldCount(); col++)
        {
            FieldDef field = dataSetToJoin.getFieldDef(col);
            
            if (fields.containsKey(field.getName()))
                continue;
            
            if (isSelective)
            {
                if (includeFields != null && !includeFields.isEmpty()
                        && !includeFields.contains(field.getName()))
                    continue;
                
                if (excludeFields != null && !excludeFields.isEmpty()
                        && excludeFields.contains(field.getName()))
                    continue;
            }
            
            fields.put(field.getName(), field);
            
            fieldsToAdd.add(col);
        }
        
        if (fields.size() == 0)
            throw new Exception(
                    EtlResource.ERROR_JOINING_DATA_SET_NO_FIELDS.getValue());
        
        DataSet dataSet = new DataSet();
        
        dataSet.setFields(fields);
        
        boolean useIndex = dataSetToJoin.getDataSetIndex() != null
                && dataSetToJoin.getDataSetIndex().size() > 0
                && Utils.removeWhiteSpace(keys).equalsIgnoreCase(
                        Utils.removeWhiteSpace(dataSetToJoin.getKeyFields()));
        
        int rows = drivingDataSet.getRecordCount();
        
        int cols = drivingDataSet.getFieldCount();
        
        Map<String, DataSetRecord> dataSetIndex = new HashMap<String, DataSetRecord>();
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = drivingDataSet.getRecord(row);
            
            String key = getKey(drivingDataSet, record, keyFields, false, true);
            
            if (Utils.isNothing(key))
                continue;
            
            DataSetRecord recordToJoin = findRecord(key, dataSetToJoin,
                    useIndex, dataSetIndex, keyFields);
            
            if (recordToJoin == null && !outer)
                continue;
            
            DataSetRecord recordToAdd = getRecordAfterInlcudeExclude(
                    drivingDataSet, record, cols, isSelective, includeFields,
                    excludeFields);
            
            for (Integer col : fieldsToAdd)
            {
                if (recordToJoin != null)
                    recordToAdd.add(recordToJoin.get(col));
                else
                    recordToAdd.add(null);
            }
            
            dataSet.addRecord(recordToAdd);
        }
        
        return dataSet;
        
    }
    
    /**
     * Performs minus operations on two data sets.
     * 
     * @param drivingDataSet the driving data set
     * @param dataSetToMinus the data set to join
     * @param keys the key fields
     * @return the data set
     * @throws Exception in case of any error
     */
    public static DataSet minus(DataSet drivingDataSet, DataSet dataSetToMinus,
            String keys)
        throws Exception
    {
        if (drivingDataSet == null)
            return null;
        
        if (dataSetToMinus == null)
            return drivingDataSet;
        
        if (drivingDataSet.isEmpty() || dataSetToMinus.isEmpty())
            return drivingDataSet;
        
        if (Utils.isNothing(keys))
            keys = drivingDataSet.getKeyFields();
        
        if (Utils.isNothing(keys))
            keys = drivingDataSet.getKeyFields();
        
        if (Utils.isNothing(keys))
            return drivingDataSet;
        
        Map<String, FieldDef> keyFields = getKeyFields(keys,
                drivingDataSet.getFields());
        
        if (keyFields.isEmpty())
            throw new Exception(
                    EtlResource.ERROR_MINUS_DATA_SET_WRONG_KEYS_IN_DRIVING
                            .getValue());
        
        keyFields = getKeyFields(keys, dataSetToMinus.getFields());
        
        if (keyFields.isEmpty())
            throw new Exception(
                    EtlResource.ERROR_MINUS_DATA_SET_WRONG_KEYS_IN_MINUS
                            .getValue());
        
        boolean useIndex = dataSetToMinus.getDataSetIndex() != null
                && dataSetToMinus.getDataSetIndex().size() > 0
                && Utils.removeWhiteSpace(keys).equalsIgnoreCase(
                        Utils.removeWhiteSpace(dataSetToMinus.getKeyFields()));
        
        Map<String, DataSetRecord> dataSetIndex = new HashMap<String, DataSetRecord>();
        
        int count = drivingDataSet.getRecordCount();
        
        DataSet dataSet = new DataSet();
        
        dataSet.setFields(drivingDataSet.getFields());
        
        DataSetData data = new DataSetData();
        
        for (int row = 0; row < count; row++)
        {
            DataSetRecord record = drivingDataSet.getRecord(row);
            
            String key = getKey(drivingDataSet, record, keyFields, false, true);
            
            if (Utils.isNothing(key))
                continue;
            
            DataSetRecord recordToMinus = findRecord(key, dataSetToMinus,
                    useIndex, dataSetIndex, keyFields);
            
            if (recordToMinus != null)
                continue;
            
            data.add(record);
        }
        
        dataSet.setData(data);
        
        return dataSet;
    }
    
    /**
     * Splits the data set on multiple data set using given key field(s).
     *
     * @param dataSet the data set
     * @param keys the keys
     * @return the linked hash map
     */
    public static LinkedHashMap<String, DataSet> split(DataSet dataSet,
            String keys)
    {
        if (dataSet == null)
            return null;
        
        LinkedHashMap<String, DataSet> map = new LinkedHashMap<String, DataSet>();
        
        if (dataSet.getRecordCount() == 0 || Utils.isNothing(keys))
        {
            map.put(LinkedHashMap.class.getName(), dataSet);
            
            return map;
        }
        
        Map<String, FieldDef> keyFields = getKeyFields(keys,
                dataSet.getFields());
        
        if (keyFields == null || keyFields.isEmpty())
        {
            map.put(LinkedHashMap.class.getName(), dataSet);
            
            return map;
        }
        
        int rows = dataSet.getRecordCount();
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = dataSet.getRecord(row);
            
            String key = getKey(dataSet, record, keyFields, false, true);
            
            if (Utils.isNothing(key))
                continue;
            
            DataSet ds = map.get(key);
            
            if (ds == null)
            {
                ds = dataSet.copy();
                
                ds.setName(Utils.makeString(dataSet.getName()) + key);
                
                DataSetData data = new DataSetData();
                
                ds.setData(data);
                
                map.put(key, ds);
            }
            
            ds.addRecord(record);
        }
        
        return map;
    }
    
    /**
     * Performs union of the two data sets.
     * 
     * @param dataSet1 the first data set
     * @param dataSet2 the second data set
     * @param keys the key fields used when unionAll == false 
     * @param unionAll if false exclude rows with duplicated keys
     * @param include the fields to include
     * @param exclude the fields to exclude
     * @return the data set
     * @throws Exception in case of any error
     */
    public static DataSet union(DataSet dataSet1, DataSet dataSet2,
            String keys, boolean unionAll, String include, String exclude)
        throws Exception
    {
        if (dataSet1 == null)
            return dataSet2;
        
        if (dataSet2 == null)
            return dataSet1;
        
        if (dataSet1.isEmpty() && dataSet2.isEmpty())
            return dataSet1;
        
        if (!dataSet1.isEmpty() && dataSet2.isEmpty())
            return dataSet1;
        
        if (dataSet1.isEmpty() && !dataSet2.isEmpty())
            return dataSet2;
        
        if (dataSet1.getFieldCount() != dataSet2.getFieldCount())
            throw new Exception(EtlResource.ERROR_UNION_DIFF_FIELDS.getValue());
        
        for (int col = 0; col < dataSet1.getFieldCount(); col++)
        {
            FieldDef field = dataSet1.getFieldDef(col);
            
            if (!field.getName().equals(dataSet2.getFieldDef(col).getName()))
                throw new Exception(
                        EtlResource.ERROR_UNION_DIFF_FIELDS.getValue());
        }
        
        Set<String> excludeFields = Utils.setSplit(exclude, ",");
        Set<String> includeFields = Utils.setSplit(include, ",");
        
        boolean isSelective = (excludeFields != null && !excludeFields
                .isEmpty())
                || (includeFields != null && !includeFields.isEmpty());
        
        ListHashMap<String, FieldDef> fields = getFieldsAfterIncludeExclude(
                dataSet1, includeFields, excludeFields);
        
        if (fields.size() == 0)
            throw new Exception(EtlResource.ERROR_UNION_NO_FIELDS.getValue());
        
        Map<String, FieldDef> keyFields = null;
        
        if (!Utils.isNothing(keys))
            keyFields = getKeyFields(keys, dataSet1.getFields());
        
        if (!unionAll && (keyFields == null || keyFields.isEmpty()))
            throw new Exception(
                    EtlResource.ERROR_UNION_NO_KEY_FIELDS.getValue());
        
        DataSet dataSet = new DataSet();
        dataSet.setFields(fields);
        
        Set<String> dataSetIndex = new HashSet<String>();
        
        int rows = dataSet1.getRecordCount();
        int cols = dataSet1.getFieldCount();
        
        boolean useIndex = false;
        
        if (!unionAll)
        {
            useIndex = dataSet1.getDataSetIndex() != null
                    && dataSet1.getDataSetIndex().size() > 0
                    && Utils.removeWhiteSpace(keys).equalsIgnoreCase(
                            Utils.removeWhiteSpace(dataSet1.getKeyFields()));
        }
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = dataSet1.getRecord(row);
            
            DataSetRecord recordToAdd = new DataSetRecord();
            
            for (int col = 0; col < cols; col++)
            {
                if (isSelective)
                {
                    FieldDef field = dataSet1.getFieldDef(col);
                    
                    if (includeFields != null && !includeFields.isEmpty()
                            && !includeFields.contains(field.getName()))
                        continue;
                    
                    if (excludeFields != null && !excludeFields.isEmpty()
                            && excludeFields.contains(field.getName()))
                        continue;
                }
                
                recordToAdd.add(record.get(col));
            }
            
            if (!unionAll && !useIndex)
            {
                String recordKey = getKey(dataSet1, record, keyFields, false,
                        true);
                
                dataSetIndex.add(recordKey);
            }
            
            dataSet.addRecord(recordToAdd);
        }
        
        rows = dataSet2.getRecordCount();
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = dataSet2.getRecord(row);
            
            DataSetRecord recordToAdd = new DataSetRecord();
            
            for (int col = 0; col < cols; col++)
            {
                if (isSelective)
                {
                    FieldDef field = dataSet2.getFieldDef(col);
                    
                    if (includeFields != null && !includeFields.isEmpty()
                            && !includeFields.contains(field.getName()))
                        continue;
                    
                    if (excludeFields != null && !excludeFields.isEmpty()
                            && excludeFields.contains(field.getName()))
                        continue;
                }
                
                recordToAdd.add(record.get(col));
            }
            
            if (!unionAll)
            {
                String recordKey = getKey(dataSet2, record, keyFields, false,
                        false);
                
                if (useIndex)
                {
                    if (dataSet1.getRecord(recordKey) != null)
                        continue;
                }
                else
                {
                    if (dataSetIndex.contains(recordKey))
                        continue;
                    
                }
                
            }
            
            dataSet.addRecord(recordToAdd);
        }
        
        return dataSet;
        
    }
    
}