/*
 * Aspect.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.aspect;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * Abstract class which implements MethodInterceptor interface. Given the generic type <code>A</code> as a parameter or instance of the <code>Class<A></code> 
 * the Aspect instantiate a new class which extends <code>A</code> but has all methods of the original class intercepted. So it is possible to write a pre or 
 * post conditions or completely replace method's code.   
 * 
 * <p>
 * The particular class which extends Aspect should override intercept. The default implementation just calls a method from the <code>A</code> class.            
 *
 * @param <A> the generic type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class Aspect<A> implements MethodInterceptor
{
    
    /** The _aspect. */
    private A _aspect;
    
    /**
     * Returns the new class which extends <code>A</code> but intercept all its methods. 
     *
     * @return the aspect
     */
    public A getAspect()
    {
        return _aspect;
    }
    
    /**
     * Instantiate the new class which extends <code>A</code> but intercept all its methods.
     *
     * @param original the original object
     */
    @SuppressWarnings("unchecked")
    public void init(A original)
    {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(original.getClass());
        enhancer.setCallback(this);
        
        _aspect = (A)enhancer.create();
    }
    
    /**
     * Instantiate the new class which extends <code>A</code> but intercept all its methods.
     *
     * @param clazz the original class
     */
    @SuppressWarnings("unchecked")
    public void init(Class<A> clazz)
    {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(clazz);
        enhancer.setCallback(this);
        
        _aspect = (A)enhancer.create();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see net.sf.cglib.proxy.MethodInterceptor#intercept(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[],
     * net.sf.cglib.proxy.MethodProxy)
     */
    public Object intercept(Object obj, Method method, Object[] args,
            MethodProxy proxy)
        throws Throwable
    {
        return proxy.invokeSuper(obj, args);
    }
}
