/*
 * MockExtendedFunctions.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.function;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.engine.LoadFunctionContext;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.Utils;

/**
 * <code>MockExtendedFunctions</code> is the class that
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @see
 * @since 1.0
 */

public class MockExtendedFunctions extends DefFunctions
{
    public static final String[] AFTER_FUNCTIONS = {"getPk", "getValue"};
    public static final String[] EXCLUDE_FUNCTIONS = {"getPk"};
    public static final String[] RUNTIME_FUNCTIONS = {"getValue"};
    
    @Override
    public String[] getAfterFunctions()
    {
        return AFTER_FUNCTIONS;
    }
    
    @Override
    public String[] getExcludeFunctions()
    {
        return EXCLUDE_FUNCTIONS;
    }
    
    @Override
    public String getFk(LoadFunctionContext context)
        throws Exception
    {
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String varName = var.getEvalName();
        String fieldName = var.getName();
        String codeVarName = dataSet.getDriver().getVarName(fieldName);
        
        Object oldValue = dataSet.getFieldValue(context.getCurrentRecord(),
                fieldName);
        
        String ifNotFound = null;
        if (Utils.isNothing(var.getValue()) || !var.isTolerate())
            ifNotFound = "select " + codeVarName + " = NULL\n";
        else
            ifNotFound = "select " + codeVarName + " = " + var.getValue()
                    + "\n";
        
        if (oldValue != null)
        {
            String s = ifNotFound + "  select top 1 " + codeVarName
                    + " = pk from #primary_keys_table where " + "field_name = "
                    + "'" + varName + "' and old_pk = " + oldValue + " \n";
            
            return s;
        }
        else
            return ifNotFound;
    }
    
    @Override
    public String getPk(LoadFunctionContext context)
        throws Exception
    {
        if (context.getScope() == Variable.EXECUTE_BEFORE)
            return getPkBefore(context);
        else if (context.getScope() == Variable.EXECUTE_AFTER)
            return getPkAfter(context);
        else
            return "";
    }
    
    private String getPkAfter(LoadFunctionContext context)
        throws Exception
    {
        Variable var = context.getVariable();
        DataSet dataSet = context.getDestination().getDataSet();
        String dataSetName = var.getTableName().toUpperCase();
        String varName = var.getEvalName();
        String oldValue = "NULL";
        String s = null;
        String sbVarName = dataSet.getDriver().getVarName(var.getName());
        
        if (var.isInclude() || !Utils.isNothing(var.getCode()))
        {
            return "   SET IDENTITY_INSERT "
                    + SqlUtils.name2RightCase(dataSet.getDriver(), dataSetName)
                    + " OFF  \n";
        }
        
        Object value = getVariableValue(context);
        if (value != null)
        {
            oldValue = dataSet.getFieldValue(context.getCurrentRecord(), varName)
                    .toString();
            
            s = "  SELECT " + sbVarName + "=@@IDENTITY\n";
            s = s
                    + "  insert into #primary_keys_table (field_name, pk, old_pk) values('"
                    + varName + "'," + sbVarName + "," + oldValue + ") \n";
        }
        else
            s = "  select " + sbVarName + " = " + oldValue + " \n";
        
        return s;
    }
    
    private String getPkBefore(LoadFunctionContext context)
        throws Exception
    {
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String varName = var.getEvalName();
        String value = getVariableValue(context);
        String oldValue = value;
        String sbVarName = dataSet.getDriver().getVarName(var.getName());
        String dataSetName = var.getTableName().toUpperCase();
        
        String s = getVarSql("select ", sbVarName, value, "=") + "\n";
        
        String sql = "";
        
        if ((var.isInclude() && value != null)
                || !Utils.isNothing(var.getCode()))
        {
            if (!Utils.isNothing(var.getCode()))
                sql = getVarSql("select ", sbVarName, var.getCode(), "=")
                        + "\n";
            
            s = s
                    + sql
                    + "   insert into #primary_keys_table (field_name, pk, old_pk) values('"
                    + varName + "'," + sbVarName + "," + oldValue + ") \n";
            s = s + "   SET IDENTITY_INSERT "
                    + SqlUtils.name2RightCase(dataSet.getDriver(), dataSetName)
                    + " ON \n";
        }
        
        return s;
    }
    
    @Override
    public String[] getRuntimeFunctions()
    {
        return RUNTIME_FUNCTIONS;
    }
    
    @Override
    public String getValue(LoadFunctionContext context)
        throws Exception
    {
        String getBlobSql = "set rowcount 1 update {TRUE_TABLE_NAME} set {TRUE_FIELD_NAME} = #external_blobs.{FIELD} from #external_blobs where {TRUE_TABLE_NAME}.{TRUE_PK_FIELD_NAME} = {PK_VAR_NAME} and field_name = '{FIELD_NAME}' and table_name = '{TABLE_NAME}' and pk={PK} set rowcount 0";
        
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String fieldName = var.getName();
        String dataSetName = dataSet.getTableName();
        String value = "NULL";
        Driver driver = dataSet.getDriver();
        
        if (Utils.isNothing(var.getCode()))
        {
            int fieldType = 0;
            
            try
            {
                fieldType = dataSet.getFieldDef(fieldName).getSqlDataType();
            }
            catch (Exception ex)
            {
                // field doesn't exist in the source database which is ok
            }
            
            if (SqlUtils.isLargeObject(fieldType)
                    && var.getLinkedVarName() != null)
            {
                if (context.getScope() == Variable.EXECUTE_RUNTIME)
                    return "null";
                else if (context.getScope() != Variable.EXECUTE_AFTER)
                    return "";
                
                Object pkValue = dataSet.getFieldValue(context.getCurrentRecord(),
                        var.getLinkedVarName());
                String pkVarName = driver.getVarName(var.getLinkedVarName());
                
                if (pkValue != null)
                {
                    if (SqlUtils.isClob(fieldType))
                        getBlobSql = Utils.findAndReplace(getBlobSql,
                                "{FIELD}", "clob_field", false);
                    else
                        getBlobSql = Utils.findAndReplace(getBlobSql,
                                "{FIELD}", "blob_field", false);
                    
                    getBlobSql = Utils.findAndReplace(getBlobSql,
                            "{FIELD_NAME}", fieldName, false);
                    getBlobSql = Utils.findAndReplace(getBlobSql,
                            "{TABLE_NAME}", dataSetName, false);
                    getBlobSql = Utils.findAndReplace(getBlobSql, "{PK}",
                            pkValue.toString(), false);
                    getBlobSql = Utils.findAndReplace(getBlobSql,
                            "{PK_VAR_NAME}", pkVarName, false);
                    getBlobSql = Utils.findAndReplace(getBlobSql,
                            "{TRUE_FIELD_NAME}",
                            SqlUtils.name2RightCase(driver, fieldName), false);
                    getBlobSql = Utils
                            .findAndReplace(
                                    getBlobSql,
                                    "{TRUE_TABLE_NAME}",
                                    SqlUtils.name2RightCase(driver, dataSetName),
                                    false);
                    getBlobSql = Utils.findAndReplace(
                            getBlobSql,
                            "{TRUE_PK_FIELD_NAME}",
                            SqlUtils.name2RightCase(driver,
                                    var.getLinkedVarName()), false);
                    
                    return getBlobSql;
                }
            }
            
            if (context.getScope() != Variable.EXECUTE_BEFORE)
                return "";
            
            value = getVariableValue(context);
        }
        else
            value = var.getCode();
        
        return "   select " + driver.getVarName(var.getName()) + " = " + value
                + " \n";
    }
}
