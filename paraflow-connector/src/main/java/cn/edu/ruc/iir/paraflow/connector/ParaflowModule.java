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

import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowMetaDataReader;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowPrestoConfig;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.facebook.presto.hadoop.$internal.com.google.common.base.Preconditions.checkArgument;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
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
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
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

        configBinder(binder).bindConfig(ParaflowPrestoConfig.class);

        binder.bind(ParaflowMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowMetaDataReader.class).in(Scopes.SINGLETON);
        binder.bind(FSFactory.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowConnector.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ParaflowPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClassLoader.class).toInstance(ParaflowPlugin.getClassLoader());

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = -2173657220571025973L;
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
