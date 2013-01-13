/*
 * MasterModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

/**
 * IdeModel
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MasterModel extends ModelImpl
{
    // dictionary
    public static final String SEARCH_STR = "searchstr";
    public static final String ROOT = "root";
    
    public Model getCurrentModel()
    {
        Root root = getRoot();
        
        return root != null ? root.getCurrentNode() : this;
    }
    
    @Getter(name = ROOT)
    public Root getRoot()
    {
        return (Root)getAttributeValue(ROOT);
    }
    
    @Getter(name = SEARCH_STR)
    public String getSearchStr()
    {
        return (String)getAttributeValue(SEARCH_STR);
    }
    
    @Setter(name = ROOT)
    public void setRoot(Root value)
    {
        setAttributeValue(ROOT, value);
    }
    
    @Setter(name = SEARCH_STR)
    public void setSearchStr(String value)
    {
        setAttributeValue(SEARCH_STR, value);
    }
}
