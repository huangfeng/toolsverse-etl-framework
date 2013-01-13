/*
 * PojoWrapper.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.pojo;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.beanutils.BeanUtils;

import com.toolsverse.mvc.model.Getter;
import com.toolsverse.mvc.model.Model;
import com.toolsverse.mvc.model.ModelImpl;
import com.toolsverse.mvc.model.Reader;
import com.toolsverse.mvc.model.Setter;
import com.toolsverse.mvc.model.Writer;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.Null;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the Model interface ModelImpl uses name\value pairs to store model attributes instead of member variables. 
 * The PojoWrapper is designed to overcome this limitation. You can pass an any object of the type P to the PojoWrapper and it will created 
 * an instance of the class which extends P but has all it's getters, setters, readers and writers intercepted. As a result it calls ModelImpl methods 
 * such as access, populate, etc right before P own methods, so view is getting notified that model has changed and model is getting updated when view is changing. 
 * 
 * Pojo stands for Plain Java Object.
 * 
 * @param <P> the generic type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class PojoWrapper<P> extends ModelImpl implements Model,
        MethodInterceptor
{
    
    /** The pojo object. */
    private P _pojo;
    
    /** The setters. */
    private final Map<Method, String> _setters;
    
    /**
     * Instantiates a new PojoWrapper.
     *
     * @param pojoClass the pojo class
     */
    public PojoWrapper(Class<P> pojoClass)
    {
        _setters = new HashMap<Method, String>();
        
        init(pojoClass);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.ModelImpl#access(java.lang.String)
     */
    @Override
    public Object access(String attributeName)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return Null.NULL;
            else
                return model.access(attributeName);
        }
        
        if (Utils.isNothing(attr.getGetter()))
            return Null.NULL;
        
        if (_pojo == null)
            return Null.NULL;
        
        Method invokeMethod = null;
        
        try
        {
            invokeMethod = _pojo.getClass().getMethod(attr.getGetter(),
                    (Class[])null);
            
            return invokeMethod.invoke(_pojo, (Object[])null);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, _pojo.getClass(), "access: "
                    + Resource.ERROR_ACCESS_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
        
    }
    
    /**
     * Gets the pojo.
     *
     * @return the pojo
     */
    public P getPojo()
    {
        return _pojo;
    }
    
    /**
     * Instantiate an obejct of the type P. Intersepts getters, setters, readres and writers. 
     *
     * @param pojoClass the pojo class
     */
    @SuppressWarnings("unchecked")
    private void init(Class<?> pojoClass)
    {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(pojoClass);
        enhancer.setCallback(this);
        _pojo = (P)enhancer.create();
        
        Method[] methods = pojoClass.getMethods();
        
        for (Method method : methods)
        {
            Annotation[] annotations = method.getDeclaredAnnotations();
            if (annotations != null)
                for (Annotation annotation : annotations)
                {
                    if (annotation instanceof Getter && isGetterMethod(method))
                    {
                        Attribute attr = getAttribute(((Getter)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Getter)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getReturnType(),
                                    method.getName(), null, attrParams);
                            
                            _attributes.put(((Getter)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setGetter(method.getName());
                            attr.setAttributeClass(method.getReturnType());
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    else if (annotation instanceof Setter
                            && isSetterMethod(method))
                    {
                        _setters.put(method, ((Setter)annotation).name());
                        
                        Attribute attr = getAttribute(((Setter)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Setter)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getParameterTypes()[0],
                                    null, method.getName(), attrParams);
                            
                            _attributes.put(((Setter)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setSetter(method.getName());
                            
                            if (attr.getAttributeClass() == null)
                                attr.setAttributeClass(method
                                        .getParameterTypes()[0]);
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    else if (annotation instanceof Reader
                            && isReaderMethod(method))
                    {
                        Attribute attr = getAttribute(((Reader)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Reader)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getParameterTypes()[0],
                                    null, null, attrParams, method.getName(),
                                    null);
                            
                            _attributes.put(((Reader)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setReader(method.getName());
                            
                            if (attr.getAttributeClass() == null)
                                attr.setAttributeClass(method
                                        .getParameterTypes()[0]);
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    else if (annotation instanceof Writer
                            && isWriterMethod(method))
                    {
                        Attribute attr = getAttribute(((Writer)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Writer)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getParameterTypes()[0],
                                    null, null, attrParams, null,
                                    method.getName());
                            
                            _attributes.put(((Writer)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setWriter(method.getName());
                            
                            if (attr.getAttributeClass() == null)
                                attr.setAttributeClass(method
                                        .getParameterTypes()[0]);
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    
                }
        }
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
        if (_setters.containsKey(method))
        {
            setAttributeValue(_setters.get(method), args[0]);
        }
        
        return proxy.invokeSuper(obj, args);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.ModelImpl#populate(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void populate(String attributeName, Object newValue)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return;
            else
            {
                model.populate(attributeName, newValue);
                
                return;
            }
        }
        
        if (Utils.isNothing(attr.getSetter()) || Null.NULL.equals(newValue))
            return;
        
        if (_pojo == null)
            return;
        
        Method invokeMethod = null;
        
        Class<?>[] parametertypes = new Class[] {attr.getAttributeClass()};
        
        try
        {
            invokeMethod = _pojo.getClass().getMethod(attr.getSetter(),
                    parametertypes);
            
            invokeMethod.invoke(_pojo, new Object[] {newValue});
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, _pojo.getClass(), "populate: "
                    + Resource.ERROR_POPULATE_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
    }
    
    /**
     * Sets the pojo.
     *
     * @param value the new pojo
     * @throws Exception in case of any error
     */
    public void setPojo(P value)
        throws Exception
    {
        BeanUtils.copyProperties(_pojo, value);
    }
}