/*
 * ExtUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext;

import com.toolsverse.util.Utils;

/**
 * The collection of static methods used by Extension framework.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExtUtils
{
    
    /**
     * Compares one ExtensionModule to another. Used by various sorting methods.
     *
     * @param extension1 the first ExtensionModule
     * @param extension2 the second ExtensionModule
     * @return 0 if first = second, 1 if first > second and -1 if second > first
     */
    public static int compareTo(ExtensionModule extension1,
            ExtensionModule extension2)
    {
        if (extension1 == null && extension2 == null)
            return 0;
        
        if (extension1 == null && extension2 != null)
            return -1;
        
        if (extension1 != null && extension2 == null)
            return 1;
        
        int ret = Utils.makeString(extension1.getType()).compareTo(
                Utils.makeString(extension2.getType()));
        
        if (ret == 0)
            return Utils.makeString(extension1.getDisplayName()).compareTo(
                    Utils.makeString(extension2.getDisplayName()));
        
        return ret;
    }
}
