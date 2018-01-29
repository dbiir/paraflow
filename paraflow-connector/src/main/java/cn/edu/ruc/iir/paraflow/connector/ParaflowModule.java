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
package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.metaserver.utils.MetaConfig;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Main binder class.
 * Bind interface to implementation class,
 * Bind class in scopes such as SINGLETON,
 * Bind class to created instance.
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowModule
implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public ParaflowModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId);
        this.typeManager = requireNonNull(typeManager);
    }

    /**
     * Contributes bindings and other configurations for this module to {@code binder}.
     *
     * @param binder binder
     */
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ParaflowConnectorId.class).toInstance(new ParaflowConnectorId(connectorId));
        binder.bind(TypeManager.class).toInstance(typeManager);

        configBinder(binder).bindConfig(MetaConfig.class);

        binder.bind(ParaflowMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FSFactory.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowConnector.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClassLoader.class).toInstance(ParaflowPlugin.getClassLoader());
    }
}
