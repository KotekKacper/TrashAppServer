package pl.put.poznan.server.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import pl.put.poznan.server.rest.TrashImage
import java.awt.Image
import java.sql.*
import java.sql.Date
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*


class DBUtils {

    private val logger = LoggerFactory.getLogger(DBUtils::class.java)
    private var dbUsername = "user" // provide the username
    private var dbPassword = "userpass" // provide the corresponding password
    private var conn: Connection? = getConnection(dbUsername, dbPassword)

    fun makeInsertStatement(tabName: String, cols: String): String {
        val varCount = cols.split(",").size
        val vars = ArrayList<String>()
        for (i in 1..varCount) {
            vars.add("?")
        }
        logger.debug("INSERT INTO ${tabName} (${cols}) VALUES (${vars.joinToString(",")})")
        return "INSERT INTO ${tabName} (${cols}) VALUES (${vars.joinToString(",")})"
    }

    fun insertReport(tabName: String, data: String, idName: String): String{
        var dataToSend: String = ""
        try{
            var cols = data.split("|")[0]
            var vals = data.split("|")[1]
            val colsArr = ArrayList(cols.split(","))
            val valsArr = ArrayList(vals.split("`"))


            val indx = colsArr.indexOf("${Tab.TRASH}.trash_types")
            val trashTypes = valsArr[indx]
            if (colsArr.contains("${Tab.TRASH}.trash_types")) {
                colsArr.remove("${Tab.TRASH}.trash_types")
                cols = colsArr.joinToString(",")
                valsArr.removeAt(indx)
                vals = valsArr.joinToString("`")
            }

            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, cols), Statement.RETURN_GENERATED_KEYS)
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (cols.split(",")[i-1] == "${Tab.TRASH}.creation_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.collection_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.trash_size"){
                    stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.user_login"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.USER} WHERE login = ?")
                    stmtFK?.setString(1, valuesToUpdate[i - 1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setString(i, valuesToUpdate[i-1])
                    } else {
                        return "ERROR: User not found in database"
                    }
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.vehicle_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.VEHICLE} WHERE id = ?")
                    stmtFK?.setInt(1, valuesToUpdate[i - 1].toInt())
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                    } else {
                        return "ERROR: Vehicle not found in database"
                    }
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.cleaningcrew_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.CLEAN_CREW} WHERE id = ?")
                    stmtFK?.setInt(1, valuesToUpdate[i - 1].toInt())
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                    } else {
                        return "ERROR: Crew not found in database"
                    }
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }

            val rowsAffected = stmt?.executeUpdate()
            val resultSet = stmt?.generatedKeys

            var idVal = 0
            if (resultSet?.next() == true) {
                val generatedId = resultSet.getInt(1)
                println("Generated ID: $generatedId")
                idVal = generatedId
            }
            logger.debug("$rowsAffected row updated.")

            if (colsArr.contains("${Tab.TRASH}.trash_types")){
                logger.debug(trashTypes)
                // remove old trashToTrashtype
                val stmt = conn?.prepareStatement("DELETE FROM ${Tab.TRASH_TO_TYPE} WHERE trash_id = ?")
                stmt?.setInt(1, idVal.toInt())
                stmt?.executeUpdate()
                for (tp in trashTypes.split(",")){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.TRASH_TYPE} WHERE typename = ?")
                    stmtFK?.setString(1, tp)
                    val rs = stmtFK?.executeQuery()
                    if (!rs!!.next()) {
                        // add new to Trashtype if not exist
                        val stmt = conn?.prepareStatement("INSERT INTO ${Tab.TRASH_TYPE}(typename) VALUES (?)")
                        stmt?.setString(1, tp)
                        stmt?.executeUpdate()
                    }
                    // add new record to trashToTrashtype
                    val stmt = conn
                        ?.prepareStatement("INSERT INTO ${Tab.TRASH_TO_TYPE}(trash_id, trashtype_name) VALUES (?, ?)")
                    stmt?.setInt(1, idVal.toInt())
                    stmt?.setString(2, tp)
                    stmt?.executeUpdate()
                }
            }

            dataToSend = idVal.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }

    fun insertGroup(tabName: String, data: String): String{
        var dataToSend: String = ""
        try{
            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val members = data.split("|")[2].split(",")

            for (member in members){
                val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.USER} WHERE login = ?")
                stmtFK?.setString(1, member)
                val rs = stmtFK?.executeQuery()
                if (rs!!.next()) {
                    continue
                } else {
                    return "ERROR: Member not found in database"
                }
            }

            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, cols), Statement.RETURN_GENERATED_KEYS)
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (cols.split(",")[i-1] == "${Tab.CLEAN_CREW}.meet_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")
            val resultSet = stmt?.generatedKeys

            var idVal = 0
            if (resultSet?.next() == true) {
                val generatedId = resultSet.getInt(1)
                println("Generated ID: $generatedId")
                idVal = generatedId
            }

            val stmtFK = conn
                ?.prepareStatement("DELETE FROM ${Tab.USER_GROUP} WHERE cleaningcrew_id = ?")
            stmtFK?.setInt(1, idVal.toInt())
            stmtFK?.executeUpdate()
            for (member in members){
                val stmtFK = conn
                    ?.prepareStatement("INSERT INTO ${Tab.USER_GROUP}(user_login, cleaningcrew_id) VALUES(?, ?)")
                stmtFK?.setString(1, member)
                stmtFK?.setInt(2, idVal.toInt())
                stmtFK?.executeUpdate()
            }

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Adding failed"
        }
        return dataToSend
    }

    fun insertPoint(tabName: String, data: String): String{
        var dataToSend: String = ""
        try{
            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, data.split("|")[0]))
            val valuesToInsert = data.split("|")[1].split("`")
            for (i in 1..valuesToInsert.size){
                logger.debug("$i : ${valuesToInsert[i-1]}")
                stmt?.setString(i, valuesToInsert[i-1])
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row inserted.")

            val trashTypes = data.split("|")[3]
            val idVal = if (data.split("|")[2] == "")
                                    data.split("|")[1].split("`")[0]
                               else data.split("|")[2]
            if (trashTypes != ""){
                logger.debug(trashTypes)
                // remove old trashToTrashtype
                val stmt = conn?.prepareStatement("DELETE FROM ${Tab.COLLECTING_POINT_TO_TYPE} WHERE trashcollectingpoint_localization = ?")
                stmt?.setString(1, idVal)
                stmt?.executeUpdate()
                for (tp in trashTypes.split(",")){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.TRASH_TYPE} WHERE typename = ?")
                    stmtFK?.setString(1, tp)
                    val rs = stmtFK?.executeQuery()
                    if (!rs!!.next()) {
                        // add new to Trashtype if not exist
                        val stmt = conn?.prepareStatement("INSERT INTO ${Tab.TRASH_TYPE}(typename) VALUES (?)")
                        stmt?.setString(1, tp)
                        stmt?.executeUpdate()
                    }
                    // add new record to trashToTrashtype
                    val stmt = conn
                        ?.prepareStatement("INSERT INTO ${Tab.COLLECTING_POINT_TO_TYPE}(trashcollectingpoint_localization, trashtype_name) VALUES (?, ?)")
                    stmt?.setString(1, idVal)
                    stmt?.setString(2, tp)
                    stmt?.executeUpdate()
                }
            }

            dataToSend = rowsAffected.toString()
        }catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        }catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Insertion failed"
        }
        return dataToSend
    }

    fun insertVehicle(tabName: String, data: String): String{
        var dataToSend: String = ""
        try{
            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, data.split("|")[0]))
            val valuesToInsert = data.split("|")[1].split("`")
            for (i in 1..valuesToInsert.size){
                logger.debug("$i : ${valuesToInsert[i-1]}")
                if (data.split("|")[0].split(",")[i-1] == "${Tab.VEHICLE}.filling"){
                    stmt?.setFloat(i, valuesToInsert[i-1].toFloat())
                } else{
                    stmt?.setString(i, valuesToInsert[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row inserted.")

            dataToSend = rowsAffected.toString()
        }catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        }catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Insertion failed"
        }
        return dataToSend
    }

    fun insertWorker(tabName: String, data: String): String{
        var dataToSend: String = ""
        try{
            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, data.split("|")[0]))
            val valuesToInsert = data.split("|")[1].split("`")
            for (i in 1..valuesToInsert.size){
                logger.debug("$i : ${valuesToInsert[i-1]}")
                if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.birthdate"){
                    val formatter: DateFormat = SimpleDateFormat("yyyy-MM-dd")
                    val myDate: java.util.Date = formatter.parse(valuesToInsert[i-1])
                    val sqlDate = Date(myDate.time)
                    stmt?.setDate(i, sqlDate)
                } else if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.company_nip"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.CLEAN_COMPANY} WHERE nip = ?")
                    stmtFK?.setString(1, valuesToInsert[i-1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()){
                        stmt?.setString(i, valuesToInsert[i-1])
                    } else{
                        return "ERROR: NIP not found in database"
                    }
                } else if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.vehicle_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.VEHICLE} WHERE id = ?")
                    stmtFK?.setString(1, valuesToInsert[i-1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()){
                        stmt?.setString(i, valuesToInsert[i-1])
                    } else{
                        return "ERROR: Vehicle not found in database"
                    }
                } else{
                    stmt?.setString(i, valuesToInsert[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row inserted.")

            dataToSend = rowsAffected.toString()
        }catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        }catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Insertion failed"
        }
        return dataToSend
    }

    fun insertAny(tabName: String, data: String): String{
        var dataToSend: String = ""
        try{
            val stmt = conn?.prepareStatement(makeInsertStatement(tabName, data.split("|")[0]))
            val valuesToInsert = data.split("|")[1].split("`")
            for (i in 1..valuesToInsert.size){
                logger.debug("$i : ${valuesToInsert[i-1]}")
                stmt?.setString(i, valuesToInsert[i-1])
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row inserted.")

            dataToSend = rowsAffected.toString()
        }catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        }catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Insertion failed"
        }
        return dataToSend
    }

    fun makeUpdateStatement(tabName: String, cols: String, idName: String, idVal: String): String{
        val colsArray = cols.split(",")
        val updates = ArrayList<String>()
        for (col in colsArray){
            updates.add("$col = ? ")
        }
        var output = "UPDATE ${tabName} SET "+updates.joinToString(", ")+"WHERE $idName = $idVal"

        logger.debug(output)
        return output
    }

    fun updateReport(tabName: String, data: String, idName: String): String{
        var dataToSend: String = ""
        try{
            conn?.autoCommit = false

            var cols = data.split("|")[0]
            var vals = data.split("|")[1]
            val idVal = data.split("|")[2]

            val colsArr = ArrayList(cols.split(","))
            val valsArr = ArrayList(vals.split("`"))
            val indx = colsArr.indexOf("${Tab.TRASH}.trash_types")
            val trashTypes = valsArr[indx]
            colsArr.remove("${Tab.TRASH}.trash_types")
            cols = colsArr.joinToString(",")
            valsArr.removeAt(indx)
            vals = valsArr.joinToString("`")

            // clean login_report, vehicle_id and cleaningcrew_id
            val statement = conn?.createStatement()
            val updateQuery = "UPDATE ${Tab.TRASH} SET user_login = NULL, vehicle_id = NULL, cleaningcrew_id = NULL," +
                                "collection_date = NULL, collection_localization = NULL WHERE id = $idVal"
            statement?.executeUpdate(updateQuery)

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName, idVal))
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (cols.split(",")[i-1] == "${Tab.TRASH}.creation_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.collection_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.collection_localization"){
                    if (valuesToUpdate[i-1] == ","){
                        stmt?.setString(i, null)
                    } else{
                        val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.TRASH_COLLECT_POINT} WHERE localization = ?")
                        stmtFK?.setString(1, valuesToUpdate[i - 1])
                        val rs = stmtFK?.executeQuery()
                        if (rs!!.next()) {
                            stmt?.setString(i, valuesToUpdate[i-1])
                        } else {
                            conn?.rollback()
                            return "ERROR: Collecting point not found in database"
                        }
                    }
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.trash_size"){
                    stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.user_login"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.USER} WHERE login = ?")
                    stmtFK?.setString(1, valuesToUpdate[i - 1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setString(i, valuesToUpdate[i-1])
                    } else {
                        conn?.rollback()
                        return "ERROR: User not found in database"
                    }
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.vehicle_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.VEHICLE} WHERE id = ?")
                    stmtFK?.setInt(1, valuesToUpdate[i - 1].toInt())
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                    } else {
                        conn?.rollback()
                        return "ERROR: Vehicle not found in database"
                    }
                } else if (cols.split(",")[i-1] == "${Tab.TRASH}.cleaningcrew_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.CLEAN_CREW} WHERE id = ?")
                    stmtFK?.setInt(1, valuesToUpdate[i - 1].toInt())
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setInt(i, valuesToUpdate[i-1].toInt())
                    } else {
                        conn?.rollback()
                        return "ERROR: Crew not found in database"
                    }
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            if (colsArr.contains("${Tab.TRASH}.trash_types")){
                logger.debug(trashTypes)
                // remove old trashToTrashtype
                val stmt = conn?.prepareStatement("DELETE FROM ${Tab.TRASH_TO_TYPE} WHERE trash_id = ?")
                stmt?.setInt(1, idVal.toInt())
                stmt?.executeUpdate()
                for (tp in trashTypes.split(",")){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.TRASH_TYPE} WHERE typename = ?")
                    stmtFK?.setString(1, tp)
                    val rs = stmtFK?.executeQuery()
                    if (!rs!!.next()) {
                        // add new to Trashtype if not exist
                        val stmt = conn?.prepareStatement("INSERT INTO ${Tab.TRASH_TYPE}(typename) VALUES (?)")
                        stmt?.setString(1, tp)
                        stmt?.executeUpdate()
                    }
                    // add new record to trashToTrashtype
                    val stmt = conn
                        ?.prepareStatement("INSERT INTO ${Tab.TRASH_TO_TYPE}(trash_id, trashtype_name) VALUES (?, ?)")
                    stmt?.setInt(1, idVal.toInt())
                    stmt?.setString(2, tp)
                    stmt?.executeUpdate()
                }
            }

            conn?.commit()
            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            conn?.rollback()
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            conn?.rollback()
            ex.printStackTrace()
            return "ERROR: Update failed"
        } finally {
            conn?.autoCommit = true
        }
        return dataToSend
    }

    fun updateGroup(tabName: String, data: String, idName: String): String{
        var dataToSend: String = ""
        try{
            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val members = data.split("|")[2].split(",")
            val idVal = data.split("|")[3]

            for (member in members){
                val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.USER} WHERE login = ?")
                stmtFK?.setString(1, member)
                val rs = stmtFK?.executeQuery()
                if (rs!!.next()) {
                    continue
                } else {
                    return "ERROR: Member not found in database"
                }
            }

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName, idVal))
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (cols.split(",")[i-1] == "${Tab.CLEAN_CREW}.meet_date"){
                    stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            val stmtFK = conn
                ?.prepareStatement("DELETE FROM ${Tab.USER_GROUP} WHERE cleaningcrew_id = ?")
            stmtFK?.setInt(1, idVal.toInt())
            stmtFK?.executeUpdate()
            for (member in members){
                val stmtFK = conn
                    ?.prepareStatement("INSERT INTO ${Tab.USER_GROUP}(user_login, cleaningcrew_id) VALUES(?, ?)")
                stmtFK?.setString(1, member)
                stmtFK?.setInt(2, idVal.toInt())
                stmtFK?.executeUpdate()
            }

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }

    fun updateCollectingPoint(tabName: String, data: String, idName: String): String {
        var dataToSend: String = ""
        try {
            val trashTypes = data.split("|")[3]
            val idVal = if (data.split("|")[2] == "")
                data.split("|")[1].split("`")[0]
            else data.split("|")[2]

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, data.split("|")[0], idName, "'$idVal'"))
            val valuesToInsert = data.split("|")[1].split("`")
            for (i in 1..valuesToInsert.size) {
                logger.debug("$i : ${valuesToInsert[i - 1]}")
                stmt?.setString(i, valuesToInsert[i - 1])
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row inserted.")

            if (trashTypes != "") {
                logger.debug(trashTypes)
                // remove old trashToTrashtype
                val stmt =
                    conn?.prepareStatement("DELETE FROM ${Tab.COLLECTING_POINT_TO_TYPE} WHERE trashcollectingpoint_localization = ?")
                stmt?.setString(1, idVal)
                stmt?.executeUpdate()
                for (tp in trashTypes.split(",")) {
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.TRASH_TYPE} WHERE typename = ?")
                    stmtFK?.setString(1, tp)
                    val rs = stmtFK?.executeQuery()
                    if (!rs!!.next()) {
                        // add new to Trashtype if not exist
                        val stmt = conn?.prepareStatement("INSERT INTO ${Tab.TRASH_TYPE}(typename) VALUES (?)")
                        stmt?.setString(1, tp)
                        stmt?.executeUpdate()
                    }
                    // add new record to trashToTrashtype
                    val stmt = conn
                        ?.prepareStatement("INSERT INTO ${Tab.COLLECTING_POINT_TO_TYPE}(trashcollectingpoint_localization, trashtype_name) VALUES (?, ?)")
                    stmt?.setString(1, idVal)
                    stmt?.setString(2, tp)
                    stmt?.executeUpdate()
                }
            }

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException) {
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch (ex: Exception) {
            ex.printStackTrace()
            return "ERROR: Insertion failed"
        }
        return dataToSend
    }

    fun updateVehicle(tabName: String, data: String, idName: String): String{
        var dataToSend: String = ""
        try{
            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val idVal = data.split("|")[2]

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName, idVal))
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (cols.split(",")[i-1] == "${Tab.VEHICLE}.filling"){
                    stmt?.setFloat(i, valuesToUpdate[i-1].toFloat())
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }

    fun updateWorker(tabName: String, data: String, idName1: String, idName2: String): String{
        var dataToSend: String = ""
        try{
            val formatter: DateFormat = SimpleDateFormat("yyyy-MM-dd")

            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val idVal1 = "'${data.split("|")[2]}'"
            val idVal2 = Date(formatter.parse(data.split("|")[3]).time)

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName1, idVal1)+" AND ${idName2} = ?")
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.birthdate"){
                    val myDate: java.util.Date = formatter.parse(valuesToUpdate[i-1])
                    val sqlDate = Date(myDate.time)
                    stmt?.setDate(i, sqlDate)
                } else if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.company_nip") {
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.CLEAN_COMPANY} WHERE nip = ?")
                    stmtFK?.setString(1, valuesToUpdate[i - 1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()) {
                        stmt?.setString(i, valuesToUpdate[i - 1])
                    } else {
                        return "ERROR: NIP not found in database"
                    }
                } else if (data.split("|")[0].split(",")[i-1] == "${Tab.WORKER}.vehicle_id"){
                    val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.VEHICLE} WHERE id = ?")
                    stmtFK?.setString(1, valuesToUpdate[i-1])
                    val rs = stmtFK?.executeQuery()
                    if (rs!!.next()){
                        stmt?.setString(i, valuesToUpdate[i-1])
                    } else{
                        return "ERROR: Vehicle not found in database"
                    }
                } else{
                    stmt?.setString(i, valuesToUpdate[i-1])
                }
            }
            stmt?.setDate(valuesToUpdate.size+1, idVal2)

            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }

    fun updateAny(tabName: String, data: String, idName: String): String{
        var dataToSend: String = ""
        try{
            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val idVal = data.split("|")[2]

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName, idVal))
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                stmt?.setString(i, valuesToUpdate[i-1])
            }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            dataToSend = rowsAffected.toString()
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }






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
        return "{? = call NewTrashFun(${valuesToUpdate})}"
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
            "getAllActiveTrash" -> getActiveTrash(data)
            "getAllCollectedTrash" -> getAllCollectedTrash(data)
            "getReports" -> getReports(data)
            "getGroups" -> getGroups(data)
            "getUsers" -> getUsers(data)
            "getUserTrash" -> getUserTrash(data)
            "getCollectingPoints" -> getCollectingPoints(data)
            "getCompanies" -> getCompanies(data)
            "getVehicles" -> getVehicles(data)
            "getWorkers" -> getWorkers(data)
            "getUserCred" -> getUserCred(data)

            "addTrash" -> addTrashByFunc(data)
            "addReport" -> addReport(data)
            "addGroup" -> addGroup(data)
            "addCollectingPoint" -> addCollectingPoint(data)
            "addUser" -> addUser(data)
            "addCompany" -> addCompany(data)
            "addVehicle" -> addVehicle(data)
            "addWorker" -> addWorker(data)
            "addUserRegister" -> addUserRegister(data)

            "updateTrash" -> updateTrash(data)
            "updateReport" -> updateReport(data)
            "updateGroup" -> updateGroup(data)
            "updateCollectingPoint" -> updateCollectingPoint(data)
            "updateUser" -> updateUser(data)
            "updateCompany" -> updateCompany(data)
            "updateVehicle" -> updateVehicle(data)
            "updateWorker" -> updateWorker(data)

            "deleteReport" -> deleteReport(data)
            "deleteGroup" -> deleteGroup(data)
            "deleteCollectingPoint" -> deleteCollectingPoint(data)
            "deleteUser" -> deleteUser(data)
            "deleteCompany" -> deleteCompany(data)
            "deleteVehicle" -> deleteVehicle(data)
            "deleteWorker" -> deleteWorker(data)
            "deleteImage" -> deleteImage(data)

            "checkUserExist" -> checkUserExist(data)
            "checkUserForLogin" -> checkUserForLogin(data)
            "callActiveTrash" -> callActiveTrash(data)
            "callArchiveTrash" -> callArchiveTrash(data)
            else -> "ERROR: function doesn't exist"
        }
    }


    private fun getActiveTrash(data: String): String{
        logger.debug(data)

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
        logger.debug(data)

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

    private fun getReports(data: String): String{
        logger.debug(data)

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            var dataForQuerry = data.split("|")[0]
            var user_login = data.split("|")[1]

            if(!user_login.equals("admin"))
                resultset = stmt!!.executeQuery(makeSelectString(dataForQuerry, Tab.TRASH,whereString = "user_login_report = '${user_login}'",orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))
            else
                resultset = stmt!!.executeQuery(makeSelectString(dataForQuerry, Tab.TRASH,orderByString = orderBy("${Tab.TRASH}.creation_date","DESC")))

            while (resultset!!.next()) {
                var id = resultset.getString("${Tab.TRASH}.id")

                var r2 = conn!!.createStatement().executeQuery("select trashtype_name from trashtotrashtype where trash_id = ${id}")
                var types = ""
                while (r2!!.next()) {types+=r2.getString("trashtype_name").plus(',');}

                dataToSend += id.plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.localization").plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.creation_date").toString().plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH}.trash_size").toString().plus(";")
                dataToSend += resultset.getTimestamp("${Tab.TRASH}.collection_date")?.toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.user_login_report")?.toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.user_login")?.toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.vehicle_id")?.toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.cleaningcrew_id")?.toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH}.collection_localization")?.toString().plus(";")
                dataToSend += types.toString().plus(";")
                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }

        return dataToSend
    }

    private fun getGroups(data: String): String{
        logger.debug(data)

        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            resultset = stmt!!.executeQuery("select cleaningcrew_id, user_login from usergroup where user_login = '${data}'")

            while (resultset!!.next()) {
                val id = resultset.getString("cleaningcrew_id")
                val resultset1 = conn!!.createStatement()
                    .executeQuery("select id, crew_name, meet_date, meeting_localization from cleaningcrew where id = ${id}")
                val resultset2 = conn!!.createStatement()
                    .executeQuery("select user_login from usergroup where cleaningcrew_id = ${id}")
                var groupMembers:String = ""
                while (resultset2!!.next()) {groupMembers+=resultset2.getString("user_login").plus(',');}

                while (resultset1!!.next()) {
                    dataToSend += id.plus(";")
                    dataToSend += resultset1.getString("crew_name").plus(";")
                    dataToSend += resultset1.getString("meet_date").plus(";")
                    dataToSend += resultset1.getString("meeting_localization").plus(";")
                    dataToSend += groupMembers.plus(";")

                    dataToSend += "|"
                }
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Adding failed"
        }

        return dataToSend
    }

    private fun getCollectingPoints(data: String): String{
        logger.debug(data)
        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()
            var joinStr = useJoin(Tab.TRASH_COLLECT_POINT, Tab.TRASH, "localization", "collection_localization")
            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.TRASH_COLLECT_POINT, stringJoin = joinStr, groupByString = groupBy("${Tab.TRASH_COLLECT_POINT}.localization")))

            //resultset = stmt!!.executeQuery("select TrashCollectingpoint.localization, TrashCollectingpoint.bus_empty, TrashCollectingpoint.processing_type from TrashCollectingPoint")

            while (resultset!!.next()) {
                val loc = resultset.getString("${Tab.TRASH_COLLECT_POINT}.localization")

                var r2 = conn!!.createStatement().executeQuery("select trashtype_name from collectingpointtotrashtype where trashcollectingpoint_localization = '${loc}'")
                var types = ""
                while (r2!!.next()) {types+=r2.getString("trashtype_name").plus(',');}

                dataToSend += loc.plus(";")
                dataToSend += resultset.getInt("${Tab.TRASH_COLLECT_POINT}.bus_empty").toString().plus(";")
                dataToSend += resultset.getString("${Tab.TRASH_COLLECT_POINT}.processing_type").plus(";")
                dataToSend += resultset.getString("GROUP_CONCAT(${Tab.TRASH}.id SEPARATOR '-')").plus(";")
                dataToSend += types
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
        logger.debug(data)

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
                dataToSend += resultset.getString("${Tab.USER}.district").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.street").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.flat_number").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.post_code").plus(";")
                dataToSend += resultset.getString("${Tab.USER}.house_number").plus(";")
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

    private fun getUserTrash(data: String): String{
        logger.debug(data)

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

    private fun getCompanies(data: String): String{
        logger.debug(data)
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
                dataToSend += resultset.getString("district").plus(";")
                dataToSend += resultset.getString("street").plus(";")
                dataToSend += resultset.getString("flat_number").plus(";")
                dataToSend += resultset.getString("post_code").plus(";")
                dataToSend += resultset.getString("house_number").plus(";")
                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun getVehicles(data: String): String{
        logger.debug(data)
        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.VEHICLE, orderByString = orderBy("id")))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("id").plus(";")
                dataToSend += resultset.getString("in_use").plus(";")
                dataToSend += resultset.getString("localization").plus(";")
                dataToSend += resultset.getString("filling")
                dataToSend += "\n"
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }

    private fun getWorkers(data: String): String{
        logger.debug(data)
        var stmt: Statement? = null
        var resultset: ResultSet? = null
        var dataToSend: String = ""
        try{
            stmt = conn!!.createStatement()

            resultset = stmt!!.executeQuery(makeSelectString(data, Tab.WORKER, orderByString = orderBy("fullname")))

            while (resultset!!.next()) {
                dataToSend += resultset.getString("fullname").plus(";")
                dataToSend += resultset.getTimestamp("birthdate").toString().plus(";")
                dataToSend += resultset.getString("job_start_time").plus(";")
                dataToSend += resultset.getString("job_end_time").plus(";")
                dataToSend += resultset.getString("company_nip").plus(";")
                dataToSend += resultset.getString("vehicle_id")
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
        logger.debug(data)

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


    private fun addTrash(data: String): String{
        logger.debug(data)
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
        logger.debug(data)
        var dataToSend: String = ""
        try{
            var imageVariableToInsert: String? = ""
            var imageValueToInsert: String? = ""
            var valueToInsert = data.split("|")[0]
            logger.debug(makeCallString(valueToInsert))
            val stmt = conn?.prepareCall(makeCallString(valueToInsert))
            stmt?.registerOutParameter(1, Types.INTEGER)
            stmt?.execute()
            logger.debug("Row inserted in Trash.")
            return stmt!!.getInt(1).toString()
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Some error occured during updating. Try again later."
        }
        return dataToSend
    }

    private fun addReport(data: String): String{
        logger.debug(data)
        val tabName = Tab.TRASH
        val idName = "id"
        val output = insertReport(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Something went wrong"
        else return output
    }

    private fun addGroup(data: String): String{
        logger.debug(data)
        val tabName = Tab.CLEAN_CREW
        val output = insertGroup(tabName, data)
        if (output == "ERROR: Duplicate key") return "ERROR: Something went wrong"
        else return output
    }

    private fun addCollectingPoint(data: String): String{
        logger.debug(data)
        val tabName = Tab.TRASH_COLLECT_POINT
        val output = insertPoint(tabName, data)
        if (output == "ERROR: Duplicate key") return "ERROR: Point already in database"
        else return output
    }

    private fun addUser(data: String): String{
        logger.debug(data)
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
        logger.debug(data)
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
        catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR:User with such login exists. \nCreate another login."
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
        return dataToSend
    }
    // Done
    private fun addCompany(data: String): String{
        logger.debug(data)
        val tabName = Tab.CLEAN_COMPANY
        val output = insertAny(tabName, data)
        if (output == "ERROR: Duplicate key") return "ERROR: NIP already in database"
        else return output
    }
    // Done
    private fun addVehicle(data: String): String{
        logger.debug(data)
        val tabName = Tab.VEHICLE
        val output = insertVehicle(tabName, data)
        if (output == "ERROR: Duplicate key") return "ERROR: Vehicle already in database"
        else return output
    }
    // Done
    private fun addWorker(data: String): String{
        logger.debug(data)
        val tabName = Tab.WORKER
        val output = insertWorker(tabName, data)
        if (output == "ERROR: Duplicate key") return "ERROR: Worker already in database"
        else return output
    }


//    private fun updateTrash(data: String): String{
//        var stmt: Statement? = null
//        var dataToSend: String = ""
//        try{
//            var imageVariableToInsert: String? = ""
//            var imageValueToInsert: String? = ""
//            stmt = conn!!.createStatement()
//            var valuesToUpdate = data.split("|")[0]
//            var whereCondition = data.split("|")[1]
//            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.TRASH,valuesToUpdate, whereCondition))
//            println("$rowsAffected row(s) updated in Trash.")
//
//            dataToSend = rowsAffected.toString()
//
//            if(rowsAffected==0)
//            {
//                dataToSend = "ERROR: Some error occured during updating. Try again later."
//            }
//        }
//        catch(ex: Exception)
//        {
//            ex.printStackTrace()
//            return "ERROR: Failed to add trash"
//        }
//        return dataToSend
//    }
fun collectTrash(tabName: String, data: String, idName: String): String{
    var dataToSend: String = ""
    try{
        conn?.autoCommit = false

        var cols = data.split("|")[0]
        var vals = data.split("|")[1]
        val idVal = data.split("|")[2]
        // clean login_report, vehicle_id and cleaningcrew_id

        val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName, idVal))
        val valuesToUpdate = vals.split("`")
        for (i in 1..valuesToUpdate.size){
            logger.debug("$i : ${valuesToUpdate[i-1]}")
            if (cols.split(",")[i-1] == "${Tab.TRASH}.collection_date"){
                stmt?.setTimestamp(i, Timestamp.valueOf(valuesToUpdate[i-1]))
            }
            else if (cols.split(",")[i-1] == "${Tab.TRASH}.user_login"){
                val stmtFK = conn?.prepareStatement("SELECT * FROM ${Tab.USER} WHERE login = ?")
                stmtFK?.setString(1, valuesToUpdate[i - 1])
                val rs = stmtFK?.executeQuery()
                if (rs!!.next()) {
                    stmt?.setString(i, valuesToUpdate[i-1])
                } else {
                    conn?.rollback()
                    return "ERROR: User not found in database"
                }
            } else{
                stmt?.setString(i, valuesToUpdate[i-1])
            }
        }
        val rowsAffected = stmt?.executeUpdate()
        logger.debug("$rowsAffected row updated.")

        conn?.commit()
        dataToSend = rowsAffected.toString()
    } catch (ex: SQLIntegrityConstraintViolationException){
        conn?.rollback()
        ex.printStackTrace()
        return "ERROR: Duplicate key"
    } catch(ex: Exception)
    {
        conn?.rollback()
        ex.printStackTrace()
        return "ERROR: Update failed"
    } finally {
        conn?.autoCommit = true
    }
    return dataToSend
}

    private fun updateTrash(data: String): String{
        logger.debug(data)
        val tabName = Tab.TRASH
        val idName = "localization"
        val output = collectTrash(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Something went wrong"
        else return output
    }

    private fun updateReport(data: String): String{
        logger.debug(data)
        val tabName = Tab.TRASH
        val idName = "id"
        val output = updateReport(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Something went wrong"
        else return output
    }

    private fun updateGroup(data: String): String{
        logger.debug(data)
        val tabName = Tab.CLEAN_CREW
        val idName = "id"
        val output = updateGroup(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Group already in database"
        else return output
    }

    private fun updateCollectingPoint(data: String): String{
        logger.debug(data)
        val tabName = Tab.TRASH_COLLECT_POINT
        val idName = "localization"
        val output = updateCollectingPoint(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Point already in database"
        else return output
    }

//    private fun updateUser(data: String): String{
//        var stmt: Statement? = null
//        var dataToSend: String = ""
//        try{
//            var imageVariableToInsert: String? = ""
//            var imageValueToInsert: String? = ""
//            stmt = conn!!.createStatement()
//            var valuesToUpdate = data.split("|")[0]
//            var whereCondition = data.split("|")[1]
//            var rowsAffected = stmt!!.executeUpdate(makeUpdateString(Tab.USER,valuesToUpdate, whereCondition))
//            println("$rowsAffected row(s) updated in User.")
//
//            dataToSend = rowsAffected.toString()
//            if(rowsAffected==0)
//            {
//                dataToSend = "ERROR: Some error occured during updating. Try again later."
//            }
//        }
//        catch(ex: Exception)
//        {
//            ex.printStackTrace()
//        }
//        return dataToSend
//    }

    private fun updateCompany(data: String): String{
        logger.debug(data)
        val tabName = Tab.CLEAN_COMPANY
        val idName = "nip"
        val output = updateAny(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Vehicle already in database"
        else return output
    }

    private fun updateVehicle(data: String): String {
        logger.debug(data)
        val tabName = Tab.VEHICLE
        val idName = "id"
        val output = updateVehicle(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: Vehicle already in database"
        else return output
    }

    private fun updateWorker(data: String): String{
        logger.debug(data)
        val tabName = Tab.WORKER
        val idName1 = "fullname"
        val idName2 = "birthdate"
        val output = updateWorker(tabName, data, idName1, idName2)
        if (output == "ERROR: Duplicate key") return "ERROR: The same name and birth date"
        else return output
    }

    private fun updateUser(data: String): String{
        logger.debug(data)
        val tabName = Tab.USER
        val idName = "login"
        val output = updateUser(tabName, data, idName)
        if (output == "ERROR: Duplicate key") return "ERROR: The same login"
        else return output
    }
    fun updateUser(tabName: String, data: String, idName1: String): String{
        var dataToSend: String = ""
        try{


            val cols = data.split("|")[0]
            val vals = data.split("|")[1]
            val idVal1 = "'${data.split("|")[2]}'"

            val stmt = conn?.prepareStatement(makeUpdateStatement(tabName, cols, idName1, idVal1))
            val valuesToUpdate = vals.split("`")
            for (i in 1..valuesToUpdate.size){
                logger.debug("$i : ${valuesToUpdate[i-1]}")
                stmt?.setString(i, valuesToUpdate[i-1])
                }
            val rowsAffected = stmt?.executeUpdate()
            logger.debug("$rowsAffected row updated.")

            dataToSend = rowsAffected.toString()
        } catch (ex: SQLIntegrityConstraintViolationException){
            ex.printStackTrace()
            return "ERROR: Duplicate key"
        } catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Update failed"
        }
        return dataToSend
    }
    private fun deleteReport(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data.split("'")[1]
            conn!!.prepareStatement("DELETE FROM ${Tab.IMAGE} WHERE trash_id = '${whereCondition}'").use { imgStmt ->

                imgStmt.executeUpdate()
            }
            conn!!.prepareStatement("DELETE FROM ${Tab.TRASH_TO_TYPE} WHERE Trash_id = '${whereCondition}'").use { imgStmt ->

                imgStmt.executeUpdate()
            }
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.TRASH, data))
            println("$rowsAffected row(s) updated in Report.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                return "ERROR: Report could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Report could not be deleted. Please, try again later."
        }
        return dataToSend
    }

    private fun deleteGroup(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()


            var whereCondition = data.split("'")[1]
            conn!!.prepareStatement("DELETE FROM ${Tab.USER_GROUP} WHERE cleaningcrew_id IN (SELECT id FROM ${Tab.CLEAN_CREW} WHERE id = ${whereCondition})").use { imgStmt ->

                imgStmt.executeUpdate()
            }

            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.CLEAN_CREW, data))
            println("$rowsAffected row(s) updated in the group.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                return "ERROR: Group could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Group could not be deleted. Please, try again later."
        }
        return dataToSend
    }

    private fun deleteCollectingPoint(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data.split("'")[1]
            conn!!.prepareStatement("DELETE FROM ${Tab.COLLECTING_POINT_TO_TYPE} WHERE trashcollectingpoint_localization IN (SELECT localization FROM ${Tab.TRASH_COLLECT_POINT} WHERE localization = '${whereCondition}')").use { imgStmt ->

                imgStmt.executeUpdate()
            }
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.TRASH_COLLECT_POINT, data))
            println("$rowsAffected row(s) updated in CoollectingPoint.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                return "ERROR: Collecting point could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Collecting point could not be deleted. Please, try again later."
        }
        return dataToSend
    }

    private fun deleteUser(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data.split("'")[1]
            var RolerowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.USER_TO_ROLE, "user_".plus(data)))
            println("$RolerowsAffected row(s) updated in User.")

            conn!!.prepareStatement("DELETE FROM ${Tab.IMAGE} WHERE trash_id IN (SELECT id FROM ${Tab.TRASH} WHERE user_login_report = '${whereCondition}')").use { imgStmt ->

                imgStmt.executeUpdate()
            }
            conn!!.prepareStatement("DELETE FROM ${Tab.TRASH_TO_TYPE} WHERE trash_id IN (SELECT id FROM ${Tab.TRASH} WHERE user_login_report = '${whereCondition}')").use { imgStmt ->

                imgStmt.executeUpdate()
            }
            conn!!.prepareStatement("DELETE FROM ${Tab.TRASH} WHERE user_login_report = '${whereCondition}'").use { trashStmt ->

                trashStmt.executeUpdate()
            }
            conn!!.prepareStatement("DELETE FROM ${Tab.USER_GROUP} WHERE user_login = '${whereCondition}'").use { trashStmt ->

                trashStmt.executeUpdate()
            }
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.USER, data))
            println("$rowsAffected row(s) updated in User.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                return "ERROR: User could not be deleted. Please, try again later."
            }
            else
                return "ERROR: User ${whereCondition} was successfully deleted."
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: User could not be deleted. Please, try again later."
        }
        return dataToSend
    }

    private fun deleteCompany(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try{

            stmt = conn!!.createStatement()

            var whereCondition = data
            var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.CLEAN_COMPANY, whereCondition))
            println("$rowsAffected row(s) updated in Company.")

            dataToSend = rowsAffected.toString()
            if(rowsAffected==0)
            {
                return "ERROR: Company could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            if(ex.message?.contains("FOREIGN")!!)
                return "ERROR: Company can't be deleted. It still has workers. Fire them and try again."

            return "ERROR: Company could not be deleted. Please, try again later."
        }
        return dataToSend
    }

    private fun deleteVehicle(data: String): String{
        var stmt: Statement? = null
        var dataToSend: String = ""
        try {

            stmt = conn!!.createStatement()

            var whereCondition = data
            var workersFK = stmt!!.executeQuery(makeSelectString("COUNT(*)", Tab.WORKER, whereString = "vehicle_"+whereCondition))
            if (workersFK!!.next()) {
                if(workersFK.getInt("COUNT(*)")>0)
                    return "ERROR: Vehicle could not be deleted. There are workers assigned to it."

                var rowsAffected = stmt!!.executeUpdate(makeDeleteString(Tab.VEHICLE, whereCondition))
                println("$rowsAffected row(s) updated in Vehicle.")

                dataToSend = rowsAffected.toString()
                if (rowsAffected == 0) {
                    return "ERROR: Vehicle could not be deleted. Please, try again later."
                }
            }



        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Vehicle could not be deleted. Please, try again later."
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
                return "ERROR: Worker could not be deleted. Please, try again later."
            }
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
            return "ERROR: Worker could not be deleted. Please, try again later."
        }
        return dataToSend
    }



    fun uploadImage(trashId: String, type: String, content: ByteArray) {
        try{
            // Prepare the SQL query
            val sql = "INSERT INTO image (trash_id, mime_type, content) VALUES (?, ?, ?)"
            val stmt = conn?.prepareStatement(sql)

            // Set the parameters for the query
            stmt?.setInt(1, trashId.toInt())
            stmt?.setString(2, type)
            stmt?.setBytes(3, content)

            // Execute the query
            stmt?.executeUpdate()
        }
        catch(ex: Exception)
        {
            ex.printStackTrace()
        }
    }

    fun getImages(trashId: String, imgNumber: String): ByteArray {
        var image: ByteArray = byteArrayOf()
        // Prepare the SQL query
        try {
            val sql = "SELECT * FROM ${Tab.IMAGE} WHERE trash_id = ? LIMIT 1 OFFSET ${imgNumber}"
            val stmt = conn?.prepareStatement(sql)
            stmt?.setInt(1, trashId.toInt())
            stmt?.executeQuery().use { resultSet ->
                while (resultSet?.next()!!) {
                    image = resultSet.getBytes("content")
                }
            }
        } catch(ex: Exception){
            ex.printStackTrace()
        }
        logger.debug(image.size.toString())
        return image
    }

    fun getImageById(imageId: String): ByteArray {
        var image: ByteArray = byteArrayOf()
        // Prepare the SQL query
        try {
            val sql = "SELECT * FROM ${Tab.IMAGE} WHERE id = ?"
            val stmt = conn?.prepareStatement(sql)
            stmt?.setInt(1, imageId.toInt())
            stmt?.executeQuery().use { resultSet ->
                while (resultSet?.next()!!) {
                    image = resultSet.getBytes("content")
                }
            }
        } catch(ex: Exception){
            ex.printStackTrace()
        }
        logger.debug(image.size.toString())
        return image
    }

    fun deleteImage(data: String): String{
        val imageId = data
        // Prepare the SQL query
        try {
            val sql = "DELETE FROM ${Tab.IMAGE} WHERE id = ?"
            val stmt = conn?.prepareStatement(sql)
            stmt?.setInt(1, imageId.toInt())
            stmt?.executeUpdate()
        } catch(ex: Exception){
            ex.printStackTrace()
            return "ERROR: Failed to delete the image"
        }
        return "Image deleted"
    }


    private fun checkUserExist(data: String): String{
        logger.debug(data)

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
        logger.debug(data)

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

    private fun callActiveTrash(data: String): String{
        try{
            val stmt = conn?.prepareCall("{? = call CurrentTrashCount()}")
            stmt?.registerOutParameter(1, Types.INTEGER)
            stmt?.execute()
            return stmt?.getInt(1).toString()
        } catch(ex: Exception){
            return "ERROR: Couldn't load active trash"
        }
    }

    private fun callArchiveTrash(data: String): String{
        try{
            val stmt = conn?.prepareCall("{? = call ArchiveTrashCount()}")
            stmt?.registerOutParameter(1, Types.INTEGER)
            stmt?.execute()
            return stmt?.getInt(1).toString()
        } catch(ex: Exception){
            return "ERROR: Couldn't load archive trash"
        }
    }

}