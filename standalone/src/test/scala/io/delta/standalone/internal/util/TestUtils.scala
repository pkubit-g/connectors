package io.delta.standalone.internal.util

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils

object TestUtils {

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
   def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile

    try f(dir) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

}
