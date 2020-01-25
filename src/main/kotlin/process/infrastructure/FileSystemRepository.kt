package process.infrastructure

import process.engine.ProcessId
import java.io.*

class FileSystemRepository(val location: String) {

    fun saveFlow(o: Any, fileName: ProcessId) {
        try {
            val fileOut = FileOutputStream("$location\\$fileName")
            val objectOut = ObjectOutputStream(fileOut)
            objectOut.writeObject(o)
            objectOut.close()
            println("The Object was succesfully written to a file")
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw RuntimeException(ex)
        }
    }

    fun <T> retrieveFlow(fileName: ProcessId): T? {
        if (File("$location\\$fileName").exists()) {
            return try {
                val fileIn = FileInputStream("$location\\$fileName")
                val objectOut = ObjectInputStream(fileIn)
                val o = objectOut.readObject()
                objectOut.close()
                println("The Object  was succesfully read from a file")
                o as T?
            } catch (ex: Exception) {
                ex.printStackTrace()
                throw RuntimeException(ex)
            }
        }
        return null
    }

    fun retrieveAllFlows(): List<Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}