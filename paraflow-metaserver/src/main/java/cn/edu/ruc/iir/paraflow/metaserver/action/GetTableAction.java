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
package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetTableAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> dbIdOp = input.getProperties("dbId");
        Optional<Object> tblNameOp = input.getProperties("tblName");
        Optional<Object> dbNameOp = input.getProperties("dbName");
        if (paramOp.isPresent()
                && dbIdOp.isPresent()
                && tblNameOp.isPresent()
                && dbNameOp.isPresent()) {
            long dbId = (long) dbIdOp.get();
            String tblName = tblNameOp.get().toString();
            String sqlStatement = SQLTemplate.getTable(dbId, tblName);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                MetaProto.TblParam tblParam = MetaProto.TblParam.newBuilder()
                        .setDbName(dbNameOp.get().toString())
                        .setTblName(tblName)
                        .setCreateTime(Long.parseLong(resultList.get(0).get(2)))
                        .setLastAccessTime(Long.parseLong(resultList.get(0).get(3)))
                        .setLocationUrl(resultList.get(0).get(4))
                        .setFiberColId(Integer.parseInt(resultList.get(0).get(6)))
                        .setTimeColId(Integer.parseInt(resultList.get(0).get(7)))
                        .setIsEmpty(false)
                        .build();
                input.setParam(tblParam);
                input.setProperties("tblId", Long.parseLong(resultList.get(0).get(0)));
                input.setProperties("userId", Long.parseLong(resultList.get(0).get(1)));
                input.setProperties("sfName", resultList.get(0).get(5));
                input.setProperties("fiberFuncName", resultList.get(0).get(8));
            }
            else {
                throw new TableNotFoundException(tblName);
            }
            return input;
        }
        else {
            throw new ActionParamNotValidException();
        }
    }
}
