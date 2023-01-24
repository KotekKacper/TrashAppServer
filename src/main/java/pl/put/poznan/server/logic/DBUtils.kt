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

    private fun makeSelectString(elements: String, tabName: String, whereString: String = "", stringJoin: String = "", orderByString: String = "", groupByString: String = ""): String{
        var whereStringComplete = if(whereString.equals("")) whereString else "WHERE ".plus(whereString)

        val sqlString = "SELECT ${elements} FROM ${tabName} ${stringJoin} ${groupByString} ${whereStringComplete} ${orderByString};"

        return sqlString;
    }

    private fun makeInsertString(tabName: String, variablesToInsert: String, valueToInsert: String) : String{
        return "INSERT INTO ${tabName} (${variablesToInsert}) VALUES (${valueToInsert})"
    }

    private fun makeUpdateString(tabName: String, valuesToUpdate: String, whereCondition:String? = "") : String{
        return "UPDATE ${tabName} SET ${valuesToUpdate} WHERE ${whereCondition}"
    }

    private fun makeCallString(valuesToUpdate: String) : String{
        return "CALL `NewTrash`(${valuesToUpdate})"
    }

    private fun useJoin(fromTabName: String, toTabName:String, joinFromKey: String, joinToKey: String):String{
        var joinString="LEFT JOIN ${toTabName} ON ${fromTabName}.${joinFromKey} = ${toTabName}.${joinToKey} "
        return joinString
    }

    private fun orderBy(elements: String, order: String = "ASC"): String{return " ORDER BY ${elements} ${order} "}

    private fun groupBy(elements: String): String{return " GROUP BY ${elements} "}

    private fun makeDeleteString(tabName: String, whereCondition: String?):String{
        return "DELETE FROM ${tabName} WHERE ${whereCondition}"
    }

    fun functionSelector(sqlFun: String, data: String): String{
        // log the parameters
        logger.debug(sqlFun)

        return when(sqlFun){
            "getCompanies" -> getCompanies(data)
            "getAllActiveTrash" -> getActiveTrash(data)
            "getAllCollectedTrash" -> getAllCollectedTrash(data)
            "getUsers" -> getUsers(data)
            "getReports" -> getReports(data)
            "getAllGroups" -> getAllGroups(data)
            "getUserTrash" -> getUserTrash(data)
            "getCollectingPoints" -> getCollectingPoints(data)
            "getUserCred" -> getUserCred(data)
            "addUser" -> addUser(data)
            "addUserRegister" -> addUserRegister(data)
            "addTrash" -> addTrashByFunc(data)
            "addReport" -> addReport(data)
            "addGroup" -> addGroup(data)
            "updateTrash" -> updateTrash(data)
            "updateUser" -> updateUser(data)
            "updateReport" -> updateReport(data)
            "updateVehicle" -> updateVehicle(data)
            "updateGroup" -> updateGroup(data)
            "updateWorker" -> updateWorker(data)
            "deleteUser" -> deleteUser(data)
            "deleteCollectingPoint" -> deleteCollectingPoint(data)
            "deleteGroup" -> deleteGroup(data)
            "deleteReport" -> deleteReport(data)
            "deleteVehicle" -> deleteVehicle(data)
            "deleteWorker" -> deleteWorker(data)
            "checkUserExist" -> checkUserExist(data)
            "checkUserForLogin" -> checkUserForLogin(data)
            else -> "Error: function doesn't exist"
        }
    }

    private fun getCompanies(data: String): String{
        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.CLEAN_COMPANY, orderByString = orderBy("email")))

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

    private fun getActiveTrash(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            var joinString = useJoin(Tab.TRASH, Tab.IMAGE,"id","trash_id")
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.TRASH, stringJoin = joinString,whereString = "${Tab.TRASH}.collection_date IS NULL", orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))


            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.TRASH}.id").plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.localization").plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.creation_date").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH}.trash_size").toString().plus(";")
                dataToSend += resultset.getBytes("${Tab.IMAGE}.content").toString().plus(";")

                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun getAllCollectedTrash(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            //var joinString = useJoin(Tab.TRASH, Tab.IMAGE,"id","trash_id")
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.TRASH,whereString = "${Tab.TRASH}.collection_date IS NOT NULL", orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))


            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.TRASH}.id").plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.localization").plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.creation_date").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH}.trash_size").toString().plus(";")
                //dataToSend += resultset.getBytes("${Tab.IMAGE}.content").toString().plus(";")

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
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.USER, stringJoin = joinString, orderByString = orderBy("login")))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.USER}.login").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.password").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.email").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.USER}.phone").toString().plus(";")
                dataToSend += resultset.getString("${Tab.USER}.fullname").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.country").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.city").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.street").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.post_code").plus(";")
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
            dataToSend = data.split("|")[0]
            var user_login = data.split("|")[1]

            if(!user_login.equals("admin"))
            resultset = stmt!!.executeQuery(makeSelectString(dataToSend, Tab.TRASH,whereString = "user_login_report = '${user_login}'",orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))
            else
                resultset = stmt!!.executeQuery(makeSelectString(dataToSend, Tab.TRASH,orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.TRASH}.id").plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.localization").plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.creation_date").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH}.trash_size").toString().plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.collection_date")?.toString().plus(";")
                //dataToSend += resultset.getBytes("${Tab.IMAGE}.content")?.toString()
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
            var joinStr = useJoin(Tab.TRASH_COLLECT_POINT, Tab.TRASH, "localization", "collection_localization")
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.TRASH_COLLECT_POINT, stringJoin = joinStr, groupByString = groupBy("${Tab.TRASH_COLLECT_POINT}.localization")))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("${Tab.TRASH_COLLECT_POINT}.localization").plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH_COLLECT_POINT}.bus_empty").toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH_COLLECT_POINT}.processing_type").plus(";")
                dataToSend += resultset.getString("GROUP_CONCAT(${Tab.TRASH}.id SEPARATOR '-')").plus(";")
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

    private fun checkUserExist(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            val dataFrom = data.split(";")
            resultset = stmt!!.executeQuery(makeSelectString("COUNT(*)", Tab.USER, "login LIKE ${dataFrom[0]}"))

            while (resultset!!.next()) {
                dataToSend = resultset.getInt("COUNT(*)").toString()
            }
            if(!dataToSend.equals("0"))
            {
                dataToSend = "ERROR: User with such login already exists."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun checkUserForLogin(data: String): String{

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            val dataFrom = data.split(", ")
            resultset = stmt!!.executeQuery(makeSelectString("COUNT(*)", Tab.USER, "login = ${dataFrom[0]} AND password = ${dataFrom[1]}"))

            while (resultset!!.next()) {
                dataToSend = resultset.getInt("COUNT(*)").toString()
            }
            if(dataToSend.equals("0"))
            {
                dataToSend = "ERROR: Login or password are incorrect."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun addUser(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            var variablesToInsert = data.split("|")[0]
            var valueToInsert = data.split("|")[1]
            var userRowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.USER,variablesToInsert, valueToInsert))
            println("$userRowsAffected row(s) inserted in ${Tab.USER}.")
            var RolerowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.USER_TO_ROLE,"user_login, role_name", "${valueToInsert.split(",")[0]}, 'USER'"))
            dataToSend = userRowsAffected.toString()
            println("$RolerowsAffected row(s) inserted in ${Tab.ROLE}.")

            if(userRowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during registration. Try again later."
            }


        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun addUserRegister(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            var variablesToInsert = data.split("|")[0]
            var valueToInsert = data.split("|")[1]
            var userRowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.USER,variablesToInsert, valueToInsert))
            println("$userRowsAffected row(s) inserted in ${Tab.USER}.")
            var RolerowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.USER_TO_ROLE,"user_login, role_name", "${valueToInsert.split(",")[0]}, 'USER'"))
            dataToSend = userRowsAffected.toString()
            println("$RolerowsAffected row(s) inserted in ${Tab.ROLE}.")

            if(userRowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during registration. Try again later."
            }


        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun addTrash(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var variablesToInsert = data.split("\n")[0]
            var valueToInsert = data.split("\n")[1]
            var rowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.TRASH,variablesToInsert, valueToInsert))
            println("$rowsAffected row(s) inserted in Trash.")

            if(data.split("\n").size > 2) {
                imageVariableToInsert = data.split("\n")[2]
                imageValueToInsert = data.split("\n")[3]
                var imageRowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.IMAGE,imageVariableToInsert, imageValueToInsert))
                println("$imageRowsAffected row(s) inserted in Image.")
            }
            dataToSend = rowsAffected.toString()
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun addTrashByFunc(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valueToInsert = data.split("|")[0]
            var rowsAffected = stmt!!.executeUpdate(makeCallString(valueToInsert))
            println("$rowsAffected row(s) inserted in Trash.")


            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun addReport(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var variablesToInsert = data.split("\n")[0]
            var valueToInsert = data.split("\n")[1]
            var rowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.TRASH,variablesToInsert, valueToInsert))
            println("$rowsAffected row(s) inserted in Trash.")

            if(data.split("\n").size > 2) {
                imageVariableToInsert = data.split("\n")[2]
                imageValueToInsert = data.split("\n")[3]
                var imageRowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.IMAGE,imageVariableToInsert, imageValueToInsert))
                println("$imageRowsAffected row(s) inserted in Image.")
            }
            dataToSend = rowsAffected.toString()
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun addGroup(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var variablesToInsert = data.split("\n")[0]
            var valueToInsert = data.split("\n")[1]
            var rowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.TRASH,variablesToInsert, valueToInsert))
            println("$rowsAffected row(s) inserted in Trash.")

            if(data.split("\n").size > 2) {
                imageVariableToInsert = data.split("\n")[2]
                imageValueToInsert = data.split("\n")[3]
                var imageRowsAffected = stmt!!.executeUpdate(makeInsertString(Tab.IMAGE,imageVariableToInsert, imageValueToInsert))
                println("$imageRowsAffected row(s) inserted in Image.")
            }
            dataToSend = rowsAffected.toString()
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun updateTrash(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.TRASH,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in Trash.")

            dataToSend = rowsAffected.toString()

            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun updateReport(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.TRASH,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in Report.")

            dataToSend = rowsAffected.toString()

            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun updateUser(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.USER,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun updateGroup(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.USER,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun updateWorker(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.USER,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun updateVehicle(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            stmt = conn!!.createStatement()
            var valuesToUpdate = data.split("|")[0]
            var whereCondition = data.split("|")[1]
            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.USER,valuesToUpdate, whereCondition))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Some error occured during updating. Try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun deleteUser(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var RolerowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.USER_TO_ROLE, "user_".plus(whereCondition)))
            println("$RolerowsAffected row(s) updated in User.")
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.USER, whereCondition))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: User could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun deleteReport(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.TRASH, whereCondition))
            println("$rowsAffected row(s) updated in Report.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Report could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun deleteCollectingPoint(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.TRASH_COLLECT_POINT, whereCondition))
            println("$rowsAffected row(s) updated in CoollectingPoint.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Collecting point could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun deleteWorker(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.WORKER, whereCondition))
            println("$rowsAffected row(s) updated in Worker.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Worker could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun deleteVehicle(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.VEHICLE, whereCondition))
            println("$rowsAffected row(s) updated in Vehicle.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Vehicle could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    private fun deleteGroup(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.CLEAN_CREW, whereCondition))
            println("$rowsAffected row(s) updated in the group.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                dataToSend = "ERROR: Group could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

}