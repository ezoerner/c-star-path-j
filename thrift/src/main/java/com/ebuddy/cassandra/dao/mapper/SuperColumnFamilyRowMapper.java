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

package com.ebuddy.cassandra.dao.mapper;

import java.util.List;

import me.prettyprint.hector.api.beans.HSuperColumn;

/**
 * @author Aliaksandr Kazlou
 */
public interface SuperColumnFamilyRowMapper<T,K,SN,N,V> {
    T mapRow(K rowKey, List<HSuperColumn<SN,N,V>> superColumns);
}
