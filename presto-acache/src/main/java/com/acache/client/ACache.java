/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.acache.client;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.spi.Plugin;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import redis.clients.jedis.BinaryJedis;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.acache.utils.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ACache
        implements Plugin
{
    private static final Logger log = Logger.get(ACache.class);
    private static final File CACHE_CONF = new File("etc/acache.properties");
    BinaryJedis jedis;
    ObjectMapper om = new ObjectMapper();
    String query;
    private int callCount;
    private String host;
    private Integer port;
    private String password;
    private String reqLogDir;

    public ACache()
    {
        this.loadRedisProperties();
        this.jedis = new BinaryJedis(this.host, this.port);
        if (!isNullOrEmpty(this.password)) {
            this.jedis.auth(this.password);
        }
    }

    public void loadRedisProperties()
    {
        if (CACHE_CONF.exists()) {
            Map<String, String> properties = null;
            try {
                properties = new HashMap<>(loadProperties(CACHE_CONF));
            }
            catch (IOException e) {
                log.error("couldn't read properties: " + e);
            }
            String host = properties.get("host");
            String port = properties.get("port");
            checkArgument(!isNullOrEmpty(host),
                    "Cache configuration %s does not contain %s", CACHE_CONF.getAbsoluteFile(), "host");
            this.host = host;
            this.port = new Integer(port);
            this.password = properties.get("password");
            this.reqLogDir = properties.get("reqLogDir");
        }
    }

    public String getQuery()
    {
        return this.query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public void cacheLastResults(QueryResults queryResults)
    {
        byte[] keyBytes = generateSetKey();
        byte[] valueBytes = this.serializeQueryResults(queryResults);
        jedis.set(keyBytes, valueBytes);
    }

    public QueryResults getCachedResults()
    {
        if (this.query == null) {
            return null;
        }
        Set<byte[]> keys = this.getQueryKeys();
        QueryResults mergedResults = mergeResults(keys);
        if (mergedResults == null) {
            return null;
        }
        return mergedResults;
    }

    public void logRequest()
    {
        writeToFile();
    }

    private byte[] generateSetKey()
    {
        callCount++;
        log.info("current callCount: " + callCount);
        StringBuilder keyBuilder = new StringBuilder(this.query);
        keyBuilder.append('_');
        keyBuilder.append(callCount);
        return keyBuilder.toString().trim().toLowerCase(Locale.US).getBytes();
    }

    private byte[] generateGetKey()
    {
        StringBuilder keyBuilder = new StringBuilder(this.query);
        return keyBuilder.append('*').toString().getBytes();
    }

    private Set<byte[]> getQueryKeys()
    {
        return jedis.keys(generateGetKey());
    }

    private QueryResults mergeResults(Set<byte[]> keys)
    {
        QueryResults mergedQueryResults = null;
        for (byte[] key : keys) {
            byte[] valueBytes = jedis.get(key);
            if (valueBytes == null) {
                continue;
            }
            QueryResults currentQueryResults = this.deserializeQueryResults(valueBytes);
            if (mergedQueryResults == null) {
                mergedQueryResults = currentQueryResults;
            }
            else {
                mergedQueryResults = mergeQueryResults(mergedQueryResults, currentQueryResults);
            }
        }
        return mergedQueryResults;
    }

    private QueryResults mergeQueryResults(QueryResults mergedQueryResults, QueryResults currentQueryResults)
    {
        Iterable<List<Object>> mergedQueryResultsData = mergedQueryResults.getData();
        Iterable<List<Object>> mergedData = Iterables.concat(mergedQueryResultsData, currentQueryResults.getData());
        //mergedData = deduplicateIterable(mergedData);
        QueryResults newMergedQueryResults = new QueryResults(
                mergedQueryResults.getId(), mergedQueryResults.getInfoUri(), mergedQueryResults.getPartialCancelUri(), null,
                mergedQueryResults.getColumns(), mergedData, mergedQueryResults.getStats(), mergedQueryResults.getError(),
                mergedQueryResults.getWarnings(), mergedQueryResults.getUpdateType(), mergedQueryResults.getUpdateCount());
        return newMergedQueryResults;
    }

    private void writeToFile()
    {
        File f = new File(this.reqLogDir);
        try {
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(this.query + ";");
            bw.newLine();
            bw.close();
        }
        catch (IOException ioe) {
            log.error("Error writing file: " + ioe);
            ioe.printStackTrace();
        }
    }

    /* TODO: Fix or Remove.
    private Iterable<List<Object>> deduplicateIterable(Iterable<List<Object>> mergedData)
    {
        Iterable<List<Object>> deduplicatedIterable = new ArrayList<List<Object>>();
        List<List<Object>> deduplicatedIterableList = (List<List<Object>>) deduplicatedIterable;
        mergedData.forEach(e -> {
            if (!deduplicatedIterableList.contains(e)) {
                deduplicatedIterableList.add(e);
            }
        });
        return deduplicatedIterable;
    }*/

    private byte[] serializeObj(Object obj)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] objBytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
            objBytes = bos.toByteArray();
        }
        catch (IOException ex) {
            log.error("Exception Occurred: " + ex);
            ex.printStackTrace();
        }
        finally {
            try {
                bos.close();
            }
            catch (IOException ex) {
                log.error("handle close exception");
            }
        }
        return objBytes;
    }

    private byte[] serializeQueryResults(QueryResults queryResults)
    {
        try {
            return om.writeValueAsBytes(queryResults);
        }
        catch (JsonProcessingException ex) {
            log.error("Exception occurred: " + ex);
            ex.printStackTrace();
        }
        return null;
    }

    private QueryResults deserializeQueryResults(byte[] queryResultsBytes)
    {
        try {
            return om.readValue(queryResultsBytes, QueryResults.class);
        }
        catch (IOException ioe) {
            log.error("Exception occurred: " + ioe);
            ioe.printStackTrace();
        }
        return null;
    }
}
