import java.io.{ByteArrayOutputStream, FileNotFoundException, PrintStream}

import com.qubole.sparklens.TestUtils
import com.qubole.sparklens.app.ReporterApp
import org.scalatest.FunSuite

import scala.util.control.Breaks._

class CompatibilitySuite extends FunSuite {

  test("should be able to report on previously generated sparklens dumps") {

    breakable {

      (1 to 100).foreach(x => { //run for the versions of sparklens output saved
        try {

          val testInput = TestUtils.getFileContents(
            s"${System.getProperty("user.dir")}/src/test/compatibility-files/version-${x}.json")

          val testOut = new ByteArrayOutputStream()
          Console.withOut(new PrintStream(testOut)) {
            ReporterApp.startAnalysersFromString(testInput)
          }
          val testOutput = testOut.toString

          val olderOutput = TestUtils.getFileContents(
            s"${System.getProperty("user.dir")}/src/test/compatibility-files/version-${x}.output")

          /* checking that some important lines of the actual run also appear on
           * running in this test using the sparklens dumps */
          olderOutput.split("\n").foreach(line => {
            assert(testOutput.contains(line))
          })
        } catch {
          case e: FileNotFoundException => break
        }
      })
    }
  }

}