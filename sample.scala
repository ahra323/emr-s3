%spark
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

//Load Bank Data
val bankText = sc.parallelize(
    IOUtils.toString(
        new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
        Charset.forName("utf8")).split("\n"))
 
case class Order(age: Integer, job: String, marital: String, education: String, amount: Integer, housing: String, campaign: String)
 
val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
    s => Order(s(0).toInt,
            s(1).replaceAll("\"", ""),
            s(2).replaceAll("\"", ""),
            s(3).replaceAll("\"", ""),
            s(5).replaceAll("\"", "").toInt,
            s(6).replaceAll("\"", ""),
            s(12).replaceAll("\"", "")
        )
).toDF()
bank.registerTempTable("bank")
bank.show()
