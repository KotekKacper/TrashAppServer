package pl.put.poznan.server.logic

class DBUtils {

    fun functionSelector(sqlFun: String, data: String): String{
        return when(sqlFun){
            "getCompanies" -> getCompanies(data)
            else -> "Error: function doesn't exist"
        }
    }

    private fun getCompanies(data: String): String{


        return "company@comp.com;;0;;;;"
    }
}