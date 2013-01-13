/*
 * ProviderModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

/**
 * ProviderModel
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ProviderModel
{
    public static final String SOME_ATTR = "someattr";
    
    private String _someAttr;
    
    @Getter(name = SOME_ATTR)
    public String getSomeAttr()
    {
        return _someAttr;
    }
    
    @Setter(name = SOME_ATTR)
    public void setSomeAttr(String value)
    {
        _someAttr = value;
    }
}
