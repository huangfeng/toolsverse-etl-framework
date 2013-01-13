/*
 * Pivot.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.Bindings;

import com.toolsverse.etl.common.CommonEtlUtils;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Script;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * This {@link Task#POST} task performs pivoting operations on data set, such as grouping, de-normalization, etc. For example there is a data set with the following fields: lastname, firstname and
 * address. There can be multiple records for the same last/first name. This task will transform source data set into data set which has exactly one record for the given lastname and firstname
 * but multiple columns for the address: address1, address2, etc. User can specify fields to display which will be calculated using JavaScript.     
 *
 * @see com.toolsverse.etl.core.engine.Task
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Pivot implements OnTask
{
    
    /** The Constant MAX_FIELDS_VAR. */
    public static final String MAX_FIELDS_VAR = "MAX";
    
    /** The Constant LEADING_FIELD_VAR. */
    public static final String LEADING_FIELD_VAR = "LEADING";
    
    /** The Constant DENORMALIZE_VAR. */
    public static final String DENORMALIZE_VAR = "DENORMALIZE";
    
    /**
     * Calculates field values for the record.
     *
     * @param script the script engine
     * @param record the data set record
     * @param groupRecord the group by data set record
     * @param groupCount the group count
     * @param combinedFields the combined fields
     * @param fields the fields
     * @throws Exception in case of any error
     */
    private void calculate(Script script, DataSetRecord record,
            DataSetRecord groupRecord, Integer groupCount,
            LinkedHashMap<String, List<String>> combinedFields,
            ListHashMap<String, FieldDef> fields)
        throws Exception
    {
        DataSetRecord recordToChange = groupRecord != null ? groupRecord
                : record;
        
        for (String name : combinedFields.keySet())
        {
            List<String> vars = combinedFields.get(name);
            
            int index;
            
            FieldDef field = fields.get(name);
            if (field != null)
            {
                index = fields.indexOf(field);
            }
            else
            {
                index = fields.size();
                
                field = new FieldDef();
                field.setName(name);
                field.setSqlDataType(Types.VARCHAR);
                field.setNativeDataType("VARCHAR");
                fields.put(name, field);
            }
            
            Bindings bindings = script.getBindings(null, "JavaScript");
            bindings.put("groupCount", groupCount);
            
            if (!vars.isEmpty())
            {
                for (String fldName : vars)
                {
                    FieldDef fld = fields.get(fldName);
                    
                    fldName = fldName.replaceAll(" ", "_");
                    
                    int fldIndex = fields.indexOf(fld);
                    
                    Object fldValue = record.get(fldIndex);
                    
                    bindings.put(fldName, fldValue);
                    
                    Object groupFldValue = null;
                    
                    if (groupRecord != null)
                    {
                        groupFldValue = groupRecord.get(fldIndex);
                    }
                    
                    bindings.put("group" + fldName, groupFldValue);
                    
                    if (index == recordToChange.size())
                    {
                        bindings.put("current" + fldName, null);
                    }
                    else
                    {
                        bindings.put("current" + fldName,
                                recordToChange.get(index));
                    }
                }
            }
            
            if (index == recordToChange.size())
            {
                recordToChange.add(null);
            }
            
            Object value = script.eval(null, bindings, name, "JavaScript");
            
            recordToChange.set(index, value);
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeBeforeEtlTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executeBeforeEtlTask(EtlConfig config, Task action)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeInlineTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task, long)
     */
    public TaskResult executeInlineTask(EtlConfig config, Task task, long index)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePostTask(com.toolsverse.
     * etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task,
     * com.toolsverse.etl.common.DataSet)
     */
    public TaskResult executePostTask(EtlConfig config, Task task,
            DataSet dataSet)
        throws Exception
    {
        if (dataSet == null || dataSet.getFieldCount() == 0
                || dataSet.getRecordCount() == 0)
            return new TaskResult(dataSet);
        
        ListHashMap<String, FieldDef> fields = dataSet.getFields();
        
        Map<String, FieldDef> keys = CommonEtlUtils.getKeyFields(task
                .getVariable(EtlConfig.KEYS_VAR).getValue(), fields);
        
        Script script = new Script();
        
        LinkedHashMap<String, List<String>> calculatedFields = getCalculatedFields(
                script, task.getVariable(EtlConfig.FIELDS_VAR).getValue(),
                fields);
        
        Set<String> exclude = Utils.setSplit(
                task.getVariable(EtlConfig.EXCLUDE_VAR).getValue(), ",");
        Set<String> include = Utils.setSplit(
                task.getVariable(EtlConfig.INCLUDE_VAR).getValue(), ",");
        
        if (keys.size() == 0 && calculatedFields.size() == 0)
            return new TaskResult(dataSet);
        
        if (calculatedFields != null && !calculatedFields.isEmpty())
        {
            for (int col = 0; col < dataSet.getFieldCount(); col++)
            {
                FieldDef field = dataSet.getFieldDef(col);
                
                if (!calculatedFields.containsKey(field.getName()))
                    field.setToDelete(true);
            }
            
        }
        
        boolean denorm = Utils.str2Boolean(task.getVariable(DENORMALIZE_VAR)
                .getValue(), false);
        
        boolean ignoreCase = Utils.str2Boolean(
                task.getVariable(EtlConfig.IGNORE_CASE_VAR).getValue(), true);
        boolean doTrim = Utils.str2Boolean(task.getVariable(EtlConfig.TRIM_VAR)
                .getValue(), true);
        
        int maxFields = Utils.str2Int(task.getVariable(MAX_FIELDS_VAR)
                .getValue(), -1);
        
        String leading = task.getVariable(LEADING_FIELD_VAR).getValue();
        
        FieldDef leadingField = fields.get(leading);
        
        Map<String, TypedKeyValue<Integer, Integer>> recs = new HashMap<String, TypedKeyValue<Integer, Integer>>();
        
        int row = 0;
        
        while (row < dataSet.getRecordCount())
        {
            DataSetRecord record = dataSet.getRecord(row);
            
            String key = CommonEtlUtils.getKey(dataSet, record, keys,
                    ignoreCase, doTrim);
            
            Integer index = null;
            
            DataSetRecord groupRecord = null;
            
            Integer groupCount = 1;
            
            if (!Utils.isNothing(key))
            {
                TypedKeyValue<Integer, Integer> keyValue = recs.get(key);
                
                if (keyValue != null)
                {
                    index = keyValue.getKey();
                    groupCount = keyValue.getValue() + 1;
                    keyValue.setValue(groupCount);
                    
                    groupRecord = dataSet.getRecord(index);
                }
            }
            
            if (!calculatedFields.isEmpty())
                calculate(script, record, groupRecord, groupCount,
                        calculatedFields, fields);
            
            if (Utils.isNothing(key))
            {
                row++;
                
                continue;
            }
            
            if (index == null)
            {
                recs.put(key, new TypedKeyValue<Integer, Integer>(row, 1));
                
                row++;
                
                continue;
            }
            
            if (denorm)
                updateFields(fields, record, dataSet.getRecord(index), keys,
                        ignoreCase, doTrim, exclude, include, maxFields,
                        leadingField);
            
            dataSet.deleteRecord(row);
        }
        
        return new TaskResult(CommonEtlUtils.denormalize(dataSet, true));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePreTask(com.toolsverse.etl
     * .core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executePreTask(EtlConfig config, Task task)
        throws Exception
    {
        return executePostTask(config, task, task.getDataSet());
    }
    
    /**
     * Creates a map of calculated fields. Key is a field name. Value is a list of field names which should be used as bind variables.
     * The list can be empty.
     *
     * @param script the instance of the <@link Script> class which holds compiled javascript for calculating field values
     * @param fields the fields
     * @param fieldDefs the map of field definitions
     * @return map of calculated fields
     * @throws Exception in case of any error
     */
    private LinkedHashMap<String, List<String>> getCalculatedFields(
            Script script, String fields,
            ListHashMap<String, FieldDef> fieldDefs)
        throws Exception
    {
        LinkedHashMap<String, List<String>> map = new LinkedHashMap<String, List<String>>();
        
        if (Utils.isNothing(fields))
            return map;
        
        String[] tokens = fields.split(";", -1);
        
        for (String token : tokens)
        {
            String[] leftright = token.split("=", -1);
            
            if (leftright.length != 2)
                continue;
            
            String left = leftright[0].trim();
            String right = leftright[1].trim();
            
            if (Utils.isNothing(left))
                continue;
            
            List<String> flds = new ArrayList<String>();
            
            LinkedHashMap<String, String> possibleVars = Script
                    .getVariables(right);
            
            for (String name : possibleVars.keySet())
                if (fieldDefs.containsKey(name))
                {
                    String newName = name.replaceAll(" ", "_");
                    
                    right = right.replaceAll("\"" + name + "\"", newName);
                    
                    flds.add(name);
                }
            script.compile(null, left, Script.any2Js(Script.parseFunctions(
                    right, PivotFunctions.instance().getPatterns())),
                    "JavaScript");
            
            map.put(left, flds);
        }
        
        return map;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#init(com.toolsverse.etl.core.config
     * .EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public void init(EtlConfig config, Task task)
        throws Exception
    {
        ListHashMap<String, Variable> vars = task.getVariables();
        if (vars == null)
        {
            vars = new ListHashMap<String, Variable>();
            task.setVariables(vars);
        }
        
        EtlUtils.addVar(task.getVariables(), EtlConfig.KEYS_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.IGNORE_CASE_VAR, "true",
                false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.TRIM_VAR, "true", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.FIELDS_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.EXCLUDE_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.INCLUDE_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), MAX_FIELDS_VAR, "-1", false);
        EtlUtils.addVar(task.getVariables(), LEADING_FIELD_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), DENORMALIZE_VAR, "false", false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isInlineTask()
     */
    public boolean isInlineTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPostTask()
     */
    public boolean isPostTask()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreEtlTask()
     */
    public boolean isPreEtlTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreTask()
     */
    public boolean isPreTask()
    {
        return false;
    }
    
    /**
     * Adds unique fields from current record to the record "up update".
     *
     * @param fields the fields
     * @param current the current record
     * @param toUpdate the record to update
     * @param keys the key fields
     * @param ignoreCase if <code>true</code> ignore char case when comparing
     * @param doTrim if <code>true</code> trim before comparing
     * @param exclude the fields to exclude
     * @param include the fields to include
     * @param maxFields the maximum number of fields. Default is -1 = no limitations
     * @param leadingField the leading field
     */
    private void updateFields(ListHashMap<String, FieldDef> fields,
            DataSetRecord current, DataSetRecord toUpdate,
            Map<String, FieldDef> keys, boolean ignoreCase, boolean doTrim,
            Set<String> exclude, Set<String> include, int maxFields,
            FieldDef leadingField)
    {
        
        for (int i = 0; i < fields.size(); i++)
        {
            FieldDef field = fields.get(i);
            
            if (field.isBlob()
                    || keys.containsKey(field.getName())
                    || (exclude != null && exclude.contains(field.getName()))
                    || (include != null && include.size() > 0 && !include
                            .contains(field.getName())))
                continue;
            
            Object value = current.get(i);
            
            int spotsInLeading = (leadingField != null && leadingField != field) ? leadingField
                    .getVersions() : -1;
            
            int spotsInField = field.getVersions();
            
            if (field.isMarkedToDelete()
                    || ((spotsInLeading < 0) && toUpdate.contains(i, value,
                            ignoreCase, doTrim)))
                continue;
            
            if (maxFields > 0 && spotsInField >= maxFields)
                continue;
            
            int spotsInRecord = toUpdate.getNumberOfVersions(i);
            
            if (spotsInLeading > 0)
            {
                int lcol = fields.indexOf(leadingField);
                
                Object lValue = current.get(lcol);
                
                int index = toUpdate.getIndexOfVersion(lcol, lValue,
                        ignoreCase, doTrim);
                
                if (index == 0)
                {
                    toUpdate.set(i, value);
                }
                else if (index > 0)
                {
                    while (spotsInField < index + 1)
                    {
                        field.addVersion();
                        spotsInField++;
                    }
                    
                    while (spotsInRecord < index + 1)
                    {
                        toUpdate.addVersion(i, null);
                        spotsInRecord++;
                    }
                    
                    toUpdate.setVersion(i, index - 1, value);
                }
            }
            else
            {
                toUpdate.addVersion(i, value);
                
                if (toUpdate.getNumberOfVersions(i) > spotsInField)
                    field.addVersion();
            }
        }
    }
}
