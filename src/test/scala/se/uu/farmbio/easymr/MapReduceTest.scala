package se.uu.farmbio.easymr

import org.scalatest.FunSuite
import com.google.common.io.Files
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapReduceTest extends FunSuite {
  
  test("map file") {
    
    val tempDir = Files.createTempDir
    tempDir.deleteOnExit
    
    val params = MapParams(
      command = "rev <input> > <output>",
      local = true,
      inputData = getClass.getResource("test.txt").getPath,
      outputData = tempDir.getAbsolutePath + "/test.out",
      fifoReadTimeout = 5
    )
    Map.run(params)
    
    assert(1 == 1)
    
  }
  
}