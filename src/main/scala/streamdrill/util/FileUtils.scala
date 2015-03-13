/*
 * Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package streamdrill.util

import java.io._
import java.util.zip.GZIPInputStream

import scala.collection.Iterator
import scala.collection.mutable.ArrayBuffer

/**
 * Helper for file reading.
 *
 * @author Mikio L. Braun
 */

object FileUtils {
  def openReader(fn: String): BufferedReader =
    if (fn.endsWith(".gz"))
      new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fn)), "UTF-8"))
    else
      new BufferedReader(new InputStreamReader(new FileInputStream(fn), "UTF-8"))

  def openObjectInputStream(fn: String): ObjectInputStream =
    if (fn.endsWith(".gz"))
      new ObjectInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(fn))))
    else
      new ObjectInputStream(new BufferedInputStream(new FileInputStream(fn)))

  def lines(fn: String) = new Iterable[String] {
    def iterator: Iterator[String] = new Iterator[String] {
      val stream = openReader(fn)
      var line = stream.readLine()

      def next(): String = {
        val result = line
        line = stream.readLine()
        if (line == null) {
          stream.close()
        }
        result
      }

      def hasNext: Boolean = line != null
    }
  }

  def characters(fn: String) = new Iterable[Char] {
    def iterator = new Iterator[Char] {
      val stream = openReader(fn)
      var ch = stream.read()

      def next(): Char = {
        val result = ch
        ch = stream.read()
        if (ch == -1) {
          stream.close()
        }
        result.toChar
      }

      def hasNext: Boolean = ch != -1
    }
  }

  def readFile(fn: String): String = {
    val BUFFER_SIZE = 16384
    val sb = new StringBuffer
    val buffer = new Array[Char](BUFFER_SIZE)
    val is = openReader(fn)
    var done = false
    while (!done) {
      val c = is.read(buffer, 0, BUFFER_SIZE)
      if (c > 0) {
        if (c < buffer.length)
          sb.append(buffer.slice(0, c))
        else
          sb.append(buffer)
      }
      done = c == -1
    }
    is.close()
    sb.toString
  }

  def foreachLine(fn: String)(block: (String) => Unit) {
    val f = openReader(fn)
    var l = f.readLine
    while (l != null) {
      block(l)
      l = f.readLine
    }
    f.close()
  }

  def readLines(fn: String): IndexedSeq[String] = {
    readLines(new FileInputStream(fn))
  }

  def glob(dir: String, regexp: String): List[String] = {
    val files = new File(dir).list(new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = name.matches(regexp)
    })
    if (files == null)
      throw new Exception("Couldn't access directory \"" + dir + "\"")
    files.map(fn => dir + "/" + fn).toList.sorted
  }

  def getResourceOrFileStream(path: String): InputStream = {
    val in = getClass.getClassLoader.getResourceAsStream(path)
    if (in != null) {
      in
    } else {
      new FileInputStream(path)
    }
  }

  def readLines(in: InputStream): IndexedSeq[String] = {
    val sb = new ArrayBuffer[String]
    val f = new BufferedReader(new InputStreamReader(in, "UTF-8"))
    var l = f.readLine
    while (l != null) {
      sb.append(l)
      l = f.readLine
    }
    f.close()
    sb.toIndexedSeq
  }

  /**
   * Return the pathless basename of a file (without extension)
   *
   * Extra: Removes ".gz" first.
   */
  def basename(fullName: String) = {
    var name = new File(fullName).getName
    if (name.endsWith(".gz")) {
      name = name.substring(0, name.length - 3)
    }
    val idx = name.lastIndexOf(".")
    if (idx == -1)
      name
    else
      name.substring(0, idx)
  }
}