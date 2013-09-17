/*
 * Copyright 2013 eBuddy B.V.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ebuddy.cassandra.property;

/**
 *  Utilities for working with PropertyValues.
 *  
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"UtilityClass", "UnusedDeclaration"})
@Deprecated
public class PropertyValueUtils {

    private PropertyValueUtils() {
    }

    /**
     * Return if the property value is null or is empty.
     * @param property the property value
     * @return true if null or empty
     */
	public static boolean isEmpty( PropertyValue<?> property ) {
		return property == null || property.isEmpty();
	}

    /**
     * Return the value of the PropertyValue parameter or
     * null if the parameter is null.
     */
    public static <T> T getValueOrNull(PropertyValue<T> propertyValue) {
        return propertyValue == null ? null : propertyValue.getValue();
    }
}
