/*
 * MockFunctions.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.function;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.engine.LoadFunctionContext;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.Utils;

/**
 * <code>MockFunctions</code> is the class that
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @see
 * @since 1.0
 */

public class MockFunctions extends DefFunctions
{
    public static final String[] AFTER_FUNCTIONS = {"getPk"};
    public static final String[] EXCLUDE_FUNCTIONS = {"getPk"};
    
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
            ifNotFound = "set " + codeVarName + " = NULL;\n";
        else
            ifNotFound = "set " + codeVarName + " = " + var.getValue() + ";\n";
        
        if (oldValue != null)
        {
            String s = ifNotFound + "   select top 1 " + codeVarName
                    + " = pk from ##primary_keys_table where "
                    + "field_name = " + "'" + varName + "' and old_pk = "
                    + oldValue + "; \n";
            
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
        String msVarName = dataSet.getDriver().getVarName(var.getName());
        
        if (var.isInclude() || !Utils.isNothing(var.getCode()))
        {
            return "   SET IDENTITY_INSERT " + dataSetName + " OFF;\n";
        }
        
        Object value = getVariableValue(context);
        if (value != null)
        {
            oldValue = dataSet.getFieldValue(context.getCurrentRecord(), varName)
                    .toString();
            
            s = "  SELECT " + msVarName + "=SCOPE_IDENTITY();\n";
            s = s
                    + "  insert into ##primary_keys_table (field_name, pk, old_pk) values('"
                    + varName + "'," + msVarName + "," + oldValue + "); \n";
        }
        else
            s = "  set " + msVarName + " = " + oldValue + "; \n";
        
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
        String msVarName = dataSet.getDriver().getVarName(var.getName());
        String dataSetName = var.getTableName().toUpperCase();
        
        String s = getVarSql("set ", msVarName, value, "=") + ";\n";
        
        String sql = "";
        
        if ((var.isInclude() && value != null)
                || !Utils.isNothing(var.getCode()))
        {
            if (!Utils.isNothing(var.getCode()))
                sql = getVarSql("set ", msVarName, var.getCode(), "=") + ";\n";
            
            s = s
                    + sql
                    + "   insert into ##primary_keys_table (field_name, pk, old_pk) values('"
                    + varName + "'," + msVarName + "," + oldValue + "); \n";
            s = s + "   SET IDENTITY_INSERT " + dataSetName + " ON;\n";
        }
        
        return s;
    }
    
    @Override
    public String[] getRuntimeFunctions()
    {
        return null;
    }
    
    @Override
    public String getValue(LoadFunctionContext context)
        throws Exception
    {
        String getBlobSql = "set {VAR_NAME} = null;\n"
                + "select top 1 {VAR_NAME} = {FIELD} from ##external_blobs where field_name = '{FIELD_NAME}' and table_name = '{TABLE_NAME}' and pk={PK};\n";
        
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String fieldName = var.getName();
        String dataSetName = dataSet.getTableName();
        String value = "NULL";
        
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
            
            if ((SqlUtils.isLargeObject(fieldType))
                    && var.getLinkedVarName() != null)
            {
                Object pkValue = dataSet.getFieldValue(context.getCurrentRecord(),
                        var.getLinkedVarName());
                
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
                    getBlobSql = Utils.findAndReplace(getBlobSql, "{VAR_NAME}",
                            dataSet.getDriver().getVarName(fieldName), false);
                    
                    return getBlobSql;
                }
            }
            
            value = getVariableValue(context);
        }
        else
            value = var.getCode();
        
        return "   set " + dataSet.getDriver().getVarName(var.getName())
                + " = " + value + "; \n";
    }
}
