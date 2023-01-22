package pl.put.poznan.server.logic

import pl.put.poznan.server.logic.Tab
import org.slf4j.LoggerFactory
import pl.put.poznan.server.rest.Controller
import java.sql.*
import java.util.*

class DBUtils {

    private val logger = LoggerFactory.getLogger(DBUtils::class.java)
    private var dbUsername = "user" // provide the username
    private var dbPassword = "userpass" // provide the corresponding password
    private var conn: Connection? = getConnection(dbUsername, dbPassword)

    private fun getConnection(username: String, password: String): Connection? {
        val connectionProps = Properties()
        connectionProps.put("user", username)
        connectionProps.put("password", password)
        try {
            return DriverManager.getConnection(
                "jdbc:" + "mysql" + "://" +
                        "127.0.0.1" +
                        ":" + "3306" + "/TrashApp" +
                        "",
                connectionProps
            )
        } catch (ex: SQLException) {
            // handle any SQL errors
            ex.printStackTrace()
        } catch (ex: Exception) {
            // handle any errors
            ex.printStackTrace()
        }
        return null
    }

    private fun makeSelectString(elements: String, tabName: String, whereString: String = "", stringJoin: String = ""): String{
        var whereStringComplete = if(whereString.equals("")) whereString else "WHERE ".plus(whereString)

        val sqlString = "SELECT ${elements} FROM ${tabName} ${stringJoin} ${whereStringComplete};"

        return sqlString;
    }
    private fun useJoin(fromTabName: String, toTabName:String, joinFromKey: String, joinToKey: String):String{
        var joinString="JOIN ${toTabName} ON ${fromTabName}.${joinFromKey} = ${toTabName}.${joinToKey} "
        return joinString
    }

    private fun executeQuery(sqlString: String) {
        var stmt: Statement? = null
        var resultset: ResultSet? = null

        try {
            stmt = conn!!.createStatement()
            resultset = stmt!!.executeQuery(sqlString)

            while (resultset!!.next()) {
                logger.debug(resultset.getString("user_login_report"))
            }
        } catch (ex: SQLException) {
            // handle any errors
            ex.printStackTrace()
        } finally {
            // release resources
            if (resultset != null) {
                try {
                    resultset.close()
                } catch (sqlEx: SQLException) {
                }

                resultset = null
            }

            if (stmt != null) {
                try {
                    stmt.close()
                } catch (sqlEx: SQLException) {
                }

                stmt = null
            }

            if (conn != null) {
                try {
                    conn!!.close()
                } catch (sqlEx: SQLException) {
                }
            }
        }
    }

    fun functionSelector(sqlFun: String, data: String): String{
        // log the parameters
        logger.debug(sqlFun)

        return when(sqlFun){
            "getCompanies" -> getCompanies(data)
            else -> "Error: function doesn't exist"
        }
    }

    private fun getCompanies(data: String): String{
        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.CLEAN_COMPANY))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("nip").plus(";")
                dataToSend += resultset.getString("email").plus(";")
                dataToSend += resultset.getInt("phone").toString().plus(";")
                dataToSend += resultset.getString("country").plus(";")
                dataToSend += resultset.getString("city").plus(";")
                dataToSend += resultset.getString("street").plus(";")
                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }
}