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

    private fun makeInsertString(tabName: String, variablesToInsert: String, valueToInsert: String) : String{
        return "INSERT INTO ${tabName} (${variablesToInsert}) VALUES (${valueToInsert})"
    }
    fun functionSelector(sqlFun: String, data: String): String{
        // log the parameters
        logger.debug(sqlFun)

        return when(sqlFun){
            "getCompanies" -> getCompanies(data)
            "getActiveTrash" -> getActiveTrash(data)
            "getUsers" -> getUsers(data)
            "getReports" -> getReports(data)
            "getAllGroups" -> getAllGroups(data)
            "getUserTrash" -> getUserTrash(data)
            "getCollectingPoints" -> getCollectingPoints(data)
            "getUserCred" -> getUserCred(data)
            "addUser" -> addUser(data)
            "addTrash" -> addTrash(data)
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

    //TODO implementaion using Procedure/Function from DB
    private fun getActiveTrash(data: String): String{

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

    private fun getUsers(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            var joinString = useJoin(Tab.USER, Tab.USER_TO_ROLE,"login","user_login").plus(useJoin(Tab.USER_TO_ROLE,Tab.ROLE,"role_name","role_name"))
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.USER, stringJoin = joinString))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.USER}.login").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.password").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.email").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.USER}.phone").toString().plus(";")
                dataToSend += resultset.getString("${Tab.USER}.fullname").plus(";")
                dataToSend += resultset.getString("${Tab.ROLE}.role_name").plus(";")
                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun getReports(data: String): String{

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

    private fun getAllGroups(data: String): String{

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

    private fun getUserTrash(data: String): String{

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

    private fun getCollectingPoints(data: String): String{

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

    private fun getUserCred(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            val dataFrom = data.split(";")
            resultset = stmt!!.executeQuery(makeSelectString("COUNT(*)", Tab.USER, "login=${dataFrom[0]} AND password=${dataFrom[1]}"))

            while (resultset!!.next()) {
                dataToSend += resultset.getInt("COUNT(*)")
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun addUser(data: String): String{return ""}

    private fun addTrash(data: String): String{return ""}

}