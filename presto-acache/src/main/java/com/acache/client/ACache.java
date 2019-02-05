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
import io.airlift.log.Logger;
import org.python.core.PyInstance;
import org.python.util.PythonInterpreter;

//import java.util.HashMap;
//import org.python.core.PyObject;
//import org.python.core.PyString;

public class ACache
        implements Plugin
{
    private static final Logger log = Logger.get(ACache.class);
    PythonInterpreter interpreter;

    public ACache()
    {
        PythonInterpreter.initialize(System.getProperties(),
                System.getProperties(), new String[0]);
        this.interpreter = new PythonInterpreter();
    }

    public void cacheLastResults(QueryResults queryResults)
    {
        //this.interpreter.set(queryResults.getId(), queryResults);
        //this.interpreter.execfile("/home/jb/mac_user_dir/Documents/code/java/hello.py");
        this.interpreter.execfile("/home/jb/mac_user_dir/bgit/scache/cache.py");
        PyInstance redis = (PyInstance) this.interpreter.eval("Redis()");
        //TODO: redis.invoke("set", new PyString(queryResults.getId()), (PyObject) queryResults);
        log.info("done executing file");
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
