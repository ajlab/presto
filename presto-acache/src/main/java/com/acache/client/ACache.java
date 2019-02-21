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
import io.airlift.log.Logger;
import redis.clients.jedis.BinaryJedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
//import org.python.core.PyInstance;
//import org.python.util.PythonInterpreter;

public class ACache
        implements Plugin
{
    private static final Logger log = Logger.get(ACache.class);
    //PythonInterpreter interpreter;
    BinaryJedis jedis = new BinaryJedis("localhost", 6379);
    ObjectMapper om = new ObjectMapper();
    String query;

    public ACache(String query)
    {
        //PythonInterpreter.initialize(System.getProperties(),
        //        System.getProperties(), new String[0]);
        //this.interpreter = new PythonInterpreter();
        this.query = query;
    }

    public void cacheLastResults(QueryResults queryResults)
    {
        byte[] keyBytes = this.query.trim().toUpperCase().getBytes();
        byte[] valueBytes = this.serializeQueryResults(queryResults);
        jedis.set(keyBytes, valueBytes);
    }

    public QueryResults getCachedResults()
    {
        byte[] valueBytes = jedis.get(this.query.trim().toUpperCase().getBytes());
        if (valueBytes == null) {
            return null;
        }
        return this.deserializeQueryResults(valueBytes);
    }
    /*public void cacheLastResultsJython(QueryResults queryResults)
    {
        byte[] queryResultsBytes = this.serializeObj(queryResults);
        this.interpreter.set("queryResults_bytes", queryResultsBytes);
        this.interpreter.execfile("/home/jb/mac_user_dir/bgit/scache/integration.py");
        PyInstance integration = (PyInstance) this.interpreter.eval("Integration" + "(" + "None" + ")");
        PyString queryId = new PyString(queryResults.getId());
        integration.invoke("cache_query", queryId);
        log.info("done executing file");
    }*/

    /*public void callJythonDemo(QueryResults queryResults)
    {
        this.interpreter.execfile("/home/jb/mac_user_dir/Documents/code/java/hello.py");
        PyInstance hello = (PyInstance) this.interpreter.eval("Hello" + "(" + "None" + ")");
        hello.invoke("run");
        log.info("done executing file");
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
/*
    private PyObject queryResultsToPyObject(QueryResults queryResults)
    {
        HashMap<String, PyObject> qr = new HashMap<String, PyObject>();
        qr.put("id", new PyString(queryResults.getId()));
        return new PyObject;
    }
*/
}
