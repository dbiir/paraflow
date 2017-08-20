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
package cn.edu.ruc.iir.paraflow.metaserver.utils;
import java.util.logging.Logger;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class TestDBConnection
{
    private static final Logger logger = Logger.getLogger(TestDBConnection.class.getName());

//    @Test
//    public void DBConnectionTest()
//    {
//        try {
//            DBConnection dbConnection = DBConnection.getConnectionInstance();
//            dbConnection.connect("org.postgresql.Driver",
//                    "jdbc:postgresql://127.0.0.1:5432/metadata",
//                    "alice",
//                    "123456"
//                    );
//            Optional optional = dbConnection.sqlQuery("select * from dbmodel");
//            ResultSet resultSet = (ResultSet) optional.get();
//            while (resultSet.next()) {
//                System.out.println(resultSet.getString(4));
//            }
//        }
//        catch (java.sql.SQLException e) {
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.out.println("DBConnectionTest java.sql.SQLException");
//            System.exit(0);
//        }
//    }
}
