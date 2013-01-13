/*
 * Null.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.Serializable;

/**
 * A class which implements a Null Object Pattern.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public final class Null implements Serializable
{
    public static Null NULL = Null.instance();
    
    private static Null INSTANCE = null;
    
    private static Null instance()
    {
        if (INSTANCE == null)
            INSTANCE = new Null();
        
        return INSTANCE;
    }
    
    private Null()
    {
    }
}
