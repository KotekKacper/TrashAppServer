package pl.put.poznan.server.logic

import org.slf4j.LoggerFactory
import pl.put.poznan.server.rest.Controller

class DBUtils {

    private val logger = LoggerFactory.getLogger(DBUtils::class.java)

    fun functionSelector(sqlFun: String, data: String): String{
        // log the parameters
        logger.debug(sqlFun)

        return when(sqlFun){
            "getCompanies" -> getCompanies(data)
            else -> "Error: function doesn't exist"
        }
    }

    private fun getCompanies(data: String): String{


        return "company@comp.com;;0;;;;"
    }
}