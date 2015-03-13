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

package streamdrill.io

import java.io.{File, RandomAccessFile}

/**
 * Something like a RandomAccessFile reader.
 *
 * Used for all read operations.
 *
 * Note that there is no type information in the file. If you read some object, whatever is in the file is
 * interpreted as the type you have said.
 *
 * @author Mikio L. Braun
 */

trait SReader {
  /**
   * Set file position to pos.
   */
  def seek(pos: Long)

  /**
   * Get current position.
   */
  def position: Long

  /**
   * Get a view to this SReader with a different offset.
   *
   * The view works just as the original object, but all seeks are modified by the offset.
   *
   * Note that as in SWriter the file position is shared, so if you seek in a view, the effect on the original
   * is undefined.
   */
  def getView(offset: Long): SReader

  /**
   * Read a byte array.
   */
  def readArray(): Array[Byte]

  /**
   * Read a string.
   */
  def readUTF(): String

  def readUTFBugFix(): String = {
    val n = readInt()
    //println("Reading string with %d characters".format(n))
    val b = new StringBuilder()
    var c = 0
    while (c < n) {
      b.append(readUTFChar())
      c += 1
    }
    b.toString()
  }

  private def readUTFCont(): Int = {
    val c = readByteAsInt()
    if ((c & 0xc0) != 0x80)
      throw new IllegalArgumentException("Broken UTF-8 String: Second byte not a continuation byte!")
    c & 0x3f
  }

  private def readByteAsInt(): Int = {
    val c = readByte()
    if (c < 0) 256 + c else c
  }

  private def readUTFChar(): String = {
    val c = readByteAsInt()
    val b: Int = if ((c & 0x80) == 0) {
      c
    } else if ((c & 0xe0) == 0xc0) {
      val c2 = readUTFCont()
      ((c & 0x1f) << 3) | c2
    } else if ((c & 0xf0) == 0xe0) {
      val c2 = readUTFCont()
      val c3 = readUTFCont()
      ((c & 0x0f) << 12) | (c2 << 6) | c3
    } else if ((c & 0xf8) == 0xf0) {
      val c2 = readUTFCont()
      val c3 = readUTFCont()
      val c4 = readUTFCont()
      ((c & 0x07) << 18) | (c2 << 12) | (c3 << 6) | c4
    } else if ((c & 0xfc) == 0xf8) {
      val c2 = readUTFCont()
      val c3 = readUTFCont()
      val c4 = readUTFCont()
      val c5 = readUTFCont()
      ((c & 0x03) << 24) | (c2 << 18) | (c3 << 12) | (c4 << 6) | c5
    } else if ((c & 0xfe) == 0xfc) {
      val c2 = readUTFCont()
      val c3 = readUTFCont()
      val c4 = readUTFCont()
      val c5 = readUTFCont()
      val c6 = readUTFCont()
      ((c & 0x01) << 30) | (c2 << 24) | (c3 << 18) | (c4 << 12) | (c5 << 6) | c6
    } else {
      throw new IllegalArgumentException("Broken UTF-8 String: First char is 0x%02x".format(c))
    }
    b.toChar.toString
  }

  /**
   * Read a double precision floating point number.
   */
  def readDouble(): Double

  /**
   * Read a single precision floating point number.
   */
  def readFloat(): Float

  /**
   * Read a long integer.
   */
  def readLong(): Long

  /**
   * Read an integer.
   */
  def readInt(): Int

  /**
   * Read a short integer.
   */
  def readShort(): Short

  /**
   * Read a byte.
   */
  def readByte(): Byte

  /**
   * Read a byte array of size len, storing starting at index off.
   */
  def read(b: Array[Byte], off: Int, len: Int)

  /**
   * Read a byte array fully into the given array.
   */
  def read(b: Array[Byte])

  def close()
}

class FileSReader(file: RandomAccessFile, offset: Long=0L) extends SReader {
  seek(0L)

  def seek(pos: Long) {
    file.seek(pos + offset)
  }

  def position: Long = file.getFilePointer - offset

  def close() { file.close() }

  def readUTF() = new String(readArray(), "UTF-8")

  def readDouble() = file.readDouble()

  def readFloat() = file.readFloat()

  def readLong() = file.readLong()

  def readInt() = file.readInt()

  def readShort() = file.readShort()

  def readByte() = file.readByte()

  def read(b: Array[Byte], off: Int, len: Int) { file.readFully(b, off, len) }

  def read(b: Array[Byte]) { file.read(b, 0, b.length) }

  def getView(off: Long): SReader = new FileSReader(file, offset + off)

  def readArray(): Array[Byte] = {
    val l = readInt()
    //println("Reading array of length %d".format(l))
    val r = new Array[Byte](l)
    read(r)
    r
  }
}

class MappedFileSReader(buffer: LargeByteBuffer, offset: Long=0L) extends SReader {
  seek(0L)

  def seek(pos: Long) { buffer.position(offset + pos) }

  def position: Long = buffer.position - offset

  def getView(off: Long) = new MappedFileSReader(buffer, offset + off)

  def readArray(): Array[Byte] = {
    val l = readInt()
    var r = new Array[Byte](l)
    read(r)
    r
  }

  def readUTF() = new String(readArray(), "UTF-8")

  def readDouble() = buffer.getDouble

  def readFloat() = buffer.getFloat

  def readLong() = buffer.getLong

  def readInt() = buffer.getInt

  def readShort() = buffer.getShort

  def readByte() = buffer.getByte

  def read(b: Array[Byte], off: Int, len: Int) {
    buffer.get(b, off, len)
  }

  def read(b: Array[Byte]) {
    buffer.get(b)
  }

  // No way to close a mapped file reader...
  def close() {}

  def eof() = buffer.position() >= buffer.size()
}

object MappedFileSReader {
  def open(fn: String): MappedFileSReader = open(new File(fn))

  def open(file: File): MappedFileSReader = new MappedFileSReader(new LargeMMappedFile(new RandomAccessFile(file, "r").getChannel))
}
