/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

class BufferingLogDeletionIterator(
    underlying: Iterator[FileStatus],
    maxTimestamp: Long,
    maxVersion: Long,
    versionGetter: Path => Long) extends Iterator[FileStatus] {/**
 * Our output iterator
 */
  private val filesToDelete = new mutable.Queue[FileStatus]()
  /**
   * Our intermediate buffer which will buffer files as long as the last file requires a timestamp
   * adjustment.
   */
  private val maybeDeleteFiles = new mutable.ArrayBuffer[FileStatus]()
  private var lastFile: FileStatus = _
  private var hasNextCalled: Boolean = false

  private def init(): Unit = {
    if (underlying.hasNext) {
      lastFile = underlying.next()
      maybeDeleteFiles.append(lastFile)
    }
  }

  init()

  /** Whether the given file can be deleted based on the version and retention timestamp input. */
  private def shouldDeleteFile(file: FileStatus): Boolean = {
    file.getModificationTime <= maxTimestamp && versionGetter(file.getPath) <= maxVersion
  }

  /**
   * Files need a time adjustment if their timestamp isn't later than the lastFile.
   */
  private def needsTimeAdjustment(file: FileStatus): Boolean = {
    versionGetter(lastFile.getPath) < versionGetter(file.getPath) &&
      lastFile.getModificationTime >= file.getModificationTime
  }

  /**
   * Enqueue the files in the buffer if the last file is safe to delete. Clears the buffer.
   */
  private def flushBuffer(): Unit = {
    if (maybeDeleteFiles.lastOption.exists(shouldDeleteFile)) {
      filesToDelete.enqueue(maybeDeleteFiles: _*)
    }
    maybeDeleteFiles.clear()
  }

  /**
   * Peeks at the next file in the iterator. Based on the next file we can have three
   * possible outcomes:
   * - The underlying iterator returned a file, which doesn't require timestamp adjustment. If
   *   the file in the buffer has expired, flush the buffer to our output queue.
   * - The underlying iterator returned a file, which requires timestamp adjustment. In this case,
   *   we add this file to the buffer and fetch the next file
   * - The underlying iterator is empty. In this case, we check the last file in the buffer. If
   *   it has expired, then flush the buffer to the output queue.
   * Once this method returns, the buffer is expected to have 1 file (last file of the
   * underlying iterator) unless the underlying iterator is fully consumed.
   */
  private def queueFilesInBuffer(): Unit = {
    var continueBuffering = true
    while (continueBuffering) {
      if (!underlying.hasNext) {
        flushBuffer()
        return
      }

      var currentFile = underlying.next()
      require(currentFile != null, "FileStatus iterator returned null")
      if (needsTimeAdjustment(currentFile)) {
        currentFile = new FileStatus(
          currentFile.getLen, currentFile.isDirectory, currentFile.getReplication,
          currentFile.getBlockSize, lastFile.getModificationTime + 1, currentFile.getPath)
        maybeDeleteFiles.append(currentFile)
      } else {
        flushBuffer()
        maybeDeleteFiles.append(currentFile)
        continueBuffering = false
      }
      lastFile = currentFile
    }
  }

  override def hasNext: Boolean = {
    hasNextCalled = true
    if (filesToDelete.isEmpty) queueFilesInBuffer()
    filesToDelete.nonEmpty
  }

  override def next(): FileStatus = {
    if (!hasNextCalled) throw new NoSuchElementException()
    hasNextCalled = false
    filesToDelete.dequeue()
  }
}
