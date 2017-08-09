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
package cn.edu.ruc.iir.paraflow.metaserver.service;

import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import io.grpc.stub.StreamObserver;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class SqlGenerator
{
    public void listDatabases(MetaProto.NoneType none, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
    }

    public void listTables(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
    }

    public void getDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.DbModel> responseStreamObserver)
    {
    }

    public void getTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.TblModel> responseStreamObserver)
    {
    }

    public void getColumn(MetaProto.DbTblColParam dbTblColParam, StreamObserver<MetaProto.ColModel> responseStreamObserver)
    {
    }

    public void createDatabase(MetaProto.DbModel dbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void createTable(MetaProto.TblModel tblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void deleteDatabase(MetaProto.DbNameParam dbNameParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void deleteTable(MetaProto.DbTblParam dbTblParam, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void renameDatabase(MetaProto.RenameDbParam renameDbModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void renameTable(MetaProto.RenameTblParam renameTblModel, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void renameColumn(MetaProto.RenameColParam renameColumn, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void createFiber(MetaProto.FiberModel fiber, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void listFiberValues(MetaProto.FiberModel fiber, StreamObserver<MetaProto.LongListType> responseStreamObserver)
    {
    }

    public void addBlockIndex(MetaProto.AddBlockIndexParam addBlockIndex, StreamObserver<MetaProto.StatusType> responseStreamObserver)
    {
    }

    public void filterBlockPathsByTime(MetaProto.FilterBlockPathsByTimeParam filterBlockPathsByTime, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
    }

    public void filterBlockPaths(MetaProto.FilterBlockPathsParam filterBlockPaths, StreamObserver<MetaProto.StringListType> responseStreamObserver)
    {
    }
}
