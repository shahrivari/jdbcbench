package ir.saeed.jdbcbench

import com.google.common.base.Stopwatch
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread


object Config {
    val host = "localhost"
    val user = "root"
    val password = "chamran"
    val db = "maria_test"
    val threads = 32
    val opPerThread = 10000
}

fun getDblessConnection(): Connection {
    return DriverManager.getConnection("jdbc:mariadb://${Config.host}:3306/?" +
                                               "useUnicode=true" +
                                               "&characterEncoding=UTF-8" +
                                               "&user=${Config.user}&password=${Config.password}")
}

fun getConnection(): Connection {
    return DriverManager.getConnection("jdbc:mariadb://${Config.host}:3306/${Config.db}?" +
                                               "useUnicode=true" +
                                               "&characterEncoding=UTF-8" +
                                               "&user=${Config.user}&password=${Config.password}")
}

fun createDb() {
    getDblessConnection().use {
        it.prepareStatement("DROP DATABASE IF EXISTS ${Config.db};").execute()
        it.prepareStatement(
                "CREATE DATABASE IF NOT EXISTS ${Config.db} DEFAULT CHARACTER SET utf8mb4;").execute()
    }

    val createTable = Int::class.java.getResource("/create_table_maria.sql").readText()
    getConnection().use {
        it.prepareStatement(createTable).execute()
    }
}

fun generateIds(): Map<Int, List<Int>> {
    val parallelism = 0 until Config.threads
    return parallelism.map { threadNum ->
        threadNum to (0 until Config.opPerThread).map { threadNum * Config.opPerThread + it }
    }.toMap()
}

fun populateTable() {
    val inserted = AtomicLong()
    val ids = generateIds()
    var connections = ids.keys.map { getConnection() }
    val threads = ids.keys.map { threadNum ->
        thread(start = false) {
            val connection = connections[threadNum]
            val ids = ids[threadNum]!!.shuffled()
            connection.use {
                ids.forEach { j ->
                    connection.prepareStatement(
                            "INSERT INTO profiles(uid, user_name, profile) VALUES " +
                                    "($j, $j, $j);").executeUpdate()
                    if (inserted.incrementAndGet() % 10_000 == 0L)
                        println("Inserted: ${inserted.get()}")
                }
            }
        }
    }
    threads.forEach { it.start() }
    threads.forEach { it.join() }
}

fun readFromTable() {
    val selected = AtomicLong()
    val ids = generateIds()
    var connections = ids.keys.map { getConnection() }
    val threads = ids.keys.map { threadNum ->
        thread(start = false) {
            val connection = connections[threadNum]
            val ids = ids[threadNum]!!.shuffled()
            connection.use {
                ids.forEach { j ->
                    connection.prepareStatement(
                            "SELECT * FROM profiles where uid = $j;").executeQuery()
                    if (selected.incrementAndGet() % 10_000 == 0L)
                        println("selected: ${selected.get()}")
                }
            }
        }
    }
    threads.forEach { it.start() }
    threads.forEach { it.join() }
}


fun main(args: Array<String>) {
    createDb()
    val stopwatch = Stopwatch.createStarted()
    populateTable()
    println("Elapsed: $stopwatch")
    println("Write QPS: ${Config.threads * Config.opPerThread.toDouble() * 1000.0 /
            stopwatch.elapsed(TimeUnit.MILLISECONDS)}")
    Thread.sleep(3000)
    stopwatch.reset().start()
    readFromTable()
    println("Elapsed: $stopwatch")
    println("Read QPS: ${Config.threads * Config.opPerThread.toDouble() * 1000.0 /
            stopwatch.elapsed(TimeUnit.MILLISECONDS)}")
}