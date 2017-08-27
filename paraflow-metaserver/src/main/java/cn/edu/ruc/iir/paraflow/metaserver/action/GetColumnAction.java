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
import cn.edu.ruc.iir.paraflow.commons.exceptions.ColumnNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.Optional;

public class GetColumnAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> tblIdOp = input.getProperties("tblId");
        Optional<Object> colNameOp = input.getProperties("colName");
        if (tblIdOp.isPresent() && colNameOp.isPresent()) {
            long tblId = (long) tblIdOp.get();
            String colName = colNameOp.get().toString();
            String sqlStatement = SQLTemplate.getColumn(
                    tblId,
                    colName);
            System.out.println("sqlStatement : " + sqlStatement);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                System.out.println("colIndex : " + Integer.parseInt(resultList.get(0).get(0)));
                System.out.println("colType : " + Integer.parseInt(resultList.get(0).get(1)));
                System.out.println("dataType : " + resultList.get(0).get(2));
                MetaProto.ColParam column = MetaProto.ColParam.newBuilder()
                        .setColIndex(Integer.parseInt(resultList.get(0).get(0)))
                        .setTblName(input.getProperties("tblName").get().toString())
                        .setColName(colName)
                        .setColType(Integer.parseInt(resultList.get(0).get(1)))
                        .setDataType(resultList.get(0).get(2))
                        .setIsEmpty(false)
                        .build();
                input.setParam(column);
            }
            else {
                throw new ColumnNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
