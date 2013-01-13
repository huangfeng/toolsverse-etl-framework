/*
 * ControllerAdapterImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.toolsverse.mvc.view.ComponentAdapter;
import com.toolsverse.mvc.view.View;
import com.toolsverse.resource.Resource;
import com.toolsverse.ui.common.Behavior;
import com.toolsverse.util.log.Logger;

/**
 * The abstract implementation of the ControllerAdapter interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class ControllerAdapterImpl implements ControllerAdapter
{
    
    /** The components. */
    private Map<String, Object> _components;
    
    /** The view action methods. */
    private Map<String, Method> _viewActionMethods;
    
    /** The convert for display methods. */
    private Map<String, Method> _convertForDisplayMethods;
    
    /** The convert for storage methods. */
    private Map<String, Method> _convertForStorageMethods;
    
    /** The enabled flag. */
    private boolean _enabled;
    
    /** The is initializing flag. */
    private boolean _isInitializing;
    
    /**
     * Instantiates a new ControllerAdapterImpl.
     */
    public ControllerAdapterImpl()
    {
        _enabled = true;
        
        _isInitializing = false;
        
        _components = new LinkedHashMap<String, Object>();
        
        _viewActionMethods = new HashMap<String, Method>();
        _convertForDisplayMethods = new HashMap<String, Method>();
        _convertForStorageMethods = new HashMap<String, Method>();
        
        Method[] methods = getClass().getMethods();
        
        for (Method method : methods)
        {
            Annotation[] annotations = method.getDeclaredAnnotations();
            if (annotations != null)
                for (Annotation annotation : annotations)
                {
                    if (annotation instanceof ViewAction
                            && isViewActionMethod(method))
                    {
                        _viewActionMethods.put(((ViewAction)annotation).name(),
                                method);
                    }
                    else if (annotation instanceof ConvertForDisplay
                            && isConvertMethod(method))
                    {
                        _convertForDisplayMethods.put(
                                ((ConvertForDisplay)annotation).name(), method);
                    }
                    else if (annotation instanceof ConvertForStorage
                            && isConvertMethod(method))
                    {
                        _convertForStorageMethods.put(
                                ((ConvertForStorage)annotation).name(), method);
                    }
                    
                }
        }
    }
    
    /**
     * Adds the component.
     *
     * @param view the view
     * @param component the component
     * @return the object
     */
    public Object addComponent(View view, Object component)
    {
        if (component instanceof Behavior
                && ((Behavior)component).getBehavior() > 0)
            return addComponent(view, component,
                    ((Behavior)component).getBehavior());
        else
            return addComponent(view, component, ComponentAdapter.NONE);
    }
    
    /**
     * Adds the component using behavior modifier.
     *
     * @param view the view
     * @param component the component
     * @param behavior the behavior modifier
     * @return the object
     */
    public Object addComponent(View view, Object component, long behavior)
    {
        String name = view.addComponent(component, behavior);
        
        if (name != null)
        {
            _components.put(name, null);
            
            return component;
        }
        
        return null;
    }
    
    /**
     * Registers itself in the view and initializes MVC.
     *
     * @param view the view
     */
    public void addControllerAdapters(View view)
    {
        view.registerControllerAdapter(this);
        
        if (view.getController() != null)
            view.getController().init(null, this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#convertForDisplay(com
     * .toolsverse.mvc.controller.Controller, java.lang.String,
     * java.lang.Object)
     */
    public Object convertForDisplay(Controller controller,
            String attributeName, Object input)
    {
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#convertForDisplayAnnotation
     * (com.toolsverse.mvc.controller.Controller, java.lang.String,
     * java.lang.Object)
     */
    public final Object convertForDisplayAnnotation(Controller controller,
            String attributeName, Object input)
    {
        Method method = _convertForDisplayMethods.get(attributeName);
        
        if (method != null)
            try
            {
                return method.invoke(this, new Object[] {controller,
                        attributeName, input});
            }
            catch (Exception ex)
            {
                Logger.log(Logger.FATAL, this, method.getName() + ":"
                        + Resource.ERROR_EXECUTING_METHOD.getValue(), ex);
            }
        
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#convertForStorage(com
     * .toolsverse.mvc.controller.Controller, java.lang.String,
     * java.lang.Object)
     */
    public Object convertForStorage(Controller controller,
            String attributeName, Object input)
    {
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#convertForStorageAnnotation
     * (com.toolsverse.mvc.controller.Controller, java.lang.String,
     * java.lang.Object)
     */
    public final Object convertForStorageAnnotation(Controller controller,
            String attributeName, Object input)
    {
        Method method = _convertForStorageMethods.get(attributeName);
        
        if (method != null)
            try
            {
                return method.invoke(this, new Object[] {controller,
                        attributeName, input});
            }
            catch (Exception ex)
            {
                Logger.log(Logger.FATAL, this, method.getName() + ":"
                        + Resource.ERROR_EXECUTING_METHOD.getValue(), ex);
            }
        
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#getAttributes()
     */
    public Map<String, Object> getAttributes()
    {
        return _components;
    }
    
    /**
     * Gets the name of the attribute using substitution map. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param name the original name
     * @param suffix the suffix
     * @param map the substitution map
     * @param exclude if == true the new name is map.containsKey(name) ? name : name + suffix otherwise map.containsKey(name) ? name + suffix : name
     * @return the name of the attribute 
     */
    private String getAttrName(String name, String suffix,
            Map<String, String> map, boolean exclude)
    {
        if (map == null)
            return name + suffix;
        
        if (exclude)
            return map.containsKey(name) ? name : name + suffix;
        else
            return map.containsKey(name) ? name + suffix : name;
    }
    
    /**
     * Checks if it is a convertForDisplay or convertForStorage method.
     *
     * @param method the method
     * @return true, if it is a convert method
     */
    private boolean isConvertMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return paramsTypes != null && paramsTypes.length == 3
                && paramsTypes[0] == Controller.class
                && paramsTypes[1] == String.class
                && paramsTypes[2] == Object.class && retType != null
                && !retType.isPrimitive() && !retType.toString().equals("void");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#isEnabled()
     */
    public boolean isEnabled()
    {
        return _enabled;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#isInitializing()
     */
    public boolean isInitializing()
    {
        return _isInitializing;
    }
    
    /**
     * Checks if it is a ViewAction method.
     *
     * @param method the method
     * @return true, if it is a ViewAction method
     */
    private boolean isViewActionMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return paramsTypes != null && paramsTypes.length == 3
                && paramsTypes[0] == Controller.class
                && paramsTypes[1] == Object.class
                && paramsTypes[2] == String.class && retType != null
                && retType.isPrimitive()
                && retType.toString().equals("boolean");
    }
    
    /**
     * Removes the component.
     *
     * @param view the view
     * @param component the component
     */
    public void removeComponent(View view, Object component)
    {
        String name = view.removeComponent(component);
        
        if (name != null)
            _components.remove(name);
    }
    
    /**
     * Removes the controller adapter.
     *
     * @param view the view
     */
    public void removeControllerAdapters(View view)
    {
        view.unRegisterControllerAdapter(this);
    }
    
    /**
     * Replaces attribute names using adapre's substitution map and a suffix. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param suffix the suffix
     */
    
    public void replaceAttrNames(String suffix)
    {
        replaceAttrNames(suffix, null, false);
    }
    
    /**
     * Replaces attribute names using given substitution map, suffix and a exclude flag. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param suffix the suffix
     * @param map the substitution map
     * @param exclude the exclude flag
     */
    public void replaceAttrNames(String suffix, Map<String, String> map,
            boolean exclude)
    {
        if (_viewActionMethods != null)
        {
            Map<String, Method> viewActionMethods = new HashMap<String, Method>();
            
            for (String name : _viewActionMethods.keySet())
            {
                viewActionMethods.put(getAttrName(name, suffix, map, exclude),
                        _viewActionMethods.get(name));
            }
            
            _viewActionMethods = viewActionMethods;
        }
        
        if (_convertForDisplayMethods != null)
        {
            Map<String, Method> convertForDisplayMethods = new HashMap<String, Method>();
            
            for (String name : _convertForDisplayMethods.keySet())
            {
                convertForDisplayMethods.put(
                        getAttrName(name, suffix, map, exclude),
                        _convertForDisplayMethods.get(name));
            }
            
            _convertForDisplayMethods = convertForDisplayMethods;
        }
        
        if (_convertForStorageMethods != null)
        {
            Map<String, Method> convertForStorageMethods = new HashMap<String, Method>();
            
            for (String name : _convertForStorageMethods.keySet())
            {
                convertForStorageMethods.put(
                        getAttrName(name, suffix, map, exclude),
                        _convertForStorageMethods.get(name));
            }
            
            _convertForStorageMethods = convertForStorageMethods;
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#setEnabled(boolean)
     */
    public void setEnabled(boolean enabled)
    {
        _enabled = enabled;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#setIsInitializing(boolean
     * )
     */
    public void setIsInitializing(boolean value)
    {
        _isInitializing = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#supportsActionAnnotation
     * (java.lang.String)
     */
    public final boolean supportsActionAnnotation(String attributeName)
    {
        return _viewActionMethods.containsKey(attributeName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#
     * supportsConvertForDisplayAnnotation(java.lang.String)
     */
    public final boolean supportsConvertForDisplayAnnotation(
            String attributeName)
    {
        return _convertForDisplayMethods.containsKey(attributeName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.ControllerAdapter#
     * supportsConvertForStorageAnnotation(java.lang.String)
     */
    public final boolean supportsConvertForStorageAnnotation(
            String attributeName)
    {
        return _convertForStorageMethods.containsKey(attributeName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#viewAction(com.toolsverse
     * .mvc.controller.Controller, java.lang.Object, java.lang.String)
     */
    public boolean viewAction(Controller controller, Object source,
            String attributeName)
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.ControllerAdapter#viewAnnotationAction(
     * com.toolsverse.mvc.controller.Controller, java.lang.Object,
     * java.lang.String)
     */
    public final boolean viewAnnotationAction(Controller controller,
            Object source, String attributeName)
    {
        Method method = _viewActionMethods.get(attributeName);
        
        if (method != null)
            try
            {
                boolean result = (Boolean)method.invoke(this, new Object[] {
                        controller, source, attributeName});
                
                return result;
            }
            catch (Exception ex)
            {
                Logger.log(Logger.FATAL, this, method.getName() + ":"
                        + Resource.ERROR_EXECUTING_METHOD.getValue(), ex);
            }
        
        return false;
    }
}
