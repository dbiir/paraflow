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
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowMetadataFactory
{
    private final ParaflowConnectorId connectorId;
    private final ParaflowMetaDataReader metaDataQuery;

    @Inject
    public ParaflowMetadataFactory(ParaflowConnectorId connectorId, ParaflowMetaDataReader metaDataQuery)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaDataQuery = requireNonNull(metaDataQuery, "metaServer is null");
    }

    public ParaflowMetadata create()
    {
        return new ParaflowMetadata(metaDataQuery, connectorId);
    }

    public void shutdown()
    {
        metaDataQuery.shutdown();
    }
}
