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
package cn.edu.ruc.iir.paraflow.connector.exception;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public enum ParaflowErrorCode
        implements ErrorCodeSupplier
{
    CONNECTOR_INIT_ERROR(0, ErrorType.INTERNAL_ERROR),
    CONNECTOR_SHUTDOWN_ERROR(1, ErrorType.INTERNAL_ERROR),
    TABLE_NOT_FOUND(4, EXTERNAL),
    FUNCTION_UNSUPPORTED(8, EXTERNAL),
    HDFS_CURSOR_ERROR(6, EXTERNAL),
    PARAFLOW_HDFS_FILE_ERROR(37, EXTERNAL),
    PARAFLOW_CONFIG_ERROR(38, EXTERNAL),
    HDFS_SPLIT_NOT_OPEN(5, EXTERNAL);

    private final ErrorCode errorCode;

    ParaflowErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0210_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
