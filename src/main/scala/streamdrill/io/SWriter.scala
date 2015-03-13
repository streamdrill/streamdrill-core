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

/**
 * Something like a RandomAccessFile.
 *
 * This class is used as a file object for all write operations.
 *
 * Note that there is no type information in the writes. Therefore, the types have to match later
 * on reading. The only additional information stored is the length of byte arrays and strings.
 *
 * @author Mikio L. Braun
 */

trait SWriter {
  /**
   * Set the file position to pos.
   */
  def seek(pos: Long)

  /**
   * Get the current position in the file.
   */
  def getFilePointer: Long

  /**
   * Get a view to this SWriter with the given offset.
   *
   * Note that the true underlying position is shared between views. If you seek here to somewhere
   * else, it affects also the base SWriter.
   */
  def getView(offset: Long): SWriter

  /**
   * Highest position written so far.
   */
  def length(): Long

  /**
   * Write a byte array.
   */
  def writeArray(b: Array[Byte])

  /**
   * Flush the buffer.
   */
  def flush()

  /**
   * Flush the buffer and close.
   */
  def close()

  /**
   * Write a byte.
   */
  def writeByte(v: Int)

  /**
   * Write a short integer.
   */
  def writeShort(v: Int)

  /**
   * Write an integer.
   */
  def writeInt(v: Int)

  /**
   * Write a long integer.
   */
  def writeLong(v: Long)

  /**
   * Write a single precision floating point number.
   */
  def writeFloat(v: Float)

  /**
   * Write a double precision floating point number.
   */
  def writeDouble(v: Double)

  /**
   * Write a string in UTF-8 encoding.
   */
  def writeUTF(s: String)
}

/**
 * A SWriter based on a RandomAccessWriter.
 */
class FileSWriter(file: RandomAccessWriter) extends SWriter {
  private var maxPointer = 0L

  def length() = maxPointer

  private def updateLength() {
    val l = getFilePointer
    if (l > maxPointer)
      maxPointer = l
  }

  def getFilePointer: Long = file.getFilePointer

  def seek(pos: Long) { file.seek(pos) }

  def flush() { file.flushBuffer() }

  def getView(off: Long): SWriter = new FileSWriterWithOffset(file, off)

  def close() { file.close() }

  def writeUTF(s: String) { writeArray(s.getBytes("UTF-8")) }

  def writeDouble(v: Double) { file.writeDouble(v); updateLength() }

  def writeFloat(v: Float) { file.writeFloat(v); updateLength() }

  def writeLong(v: Long) { file.writeLong(v); updateLength() }

  def writeInt(v: Int) { file.writeInt(v); updateLength() }

  def writeShort(v: Int) { file.writeShort(v); updateLength() }

  def writeByte(v: Int) { file.writeByte(v); updateLength() }

  def write(b: Array[Byte], off: Int, len: Int) { file.write(b, off, len); updateLength() }

  def write(b: Array[Byte]) { file.write(b); updateLength() }

  def writeArray(b: Array[Byte]) {
    writeInt(b.length)
    write(b)
  }
}

/**
 * A SWriter based on an RandomAccessWriter with offset.
 *
 * This class is used to implement views for FileSWriter objects.
 */
class FileSWriterWithOffset(file: RandomAccessWriter, offset: Long) extends FileSWriter(file) {
  override def getFilePointer: Long = file.getFilePointer - offset

  override def seek(pos: Long) { file.seek(pos + offset) }

  override def getView(off: Long): SWriter = new FileSWriterWithOffset(file, offset + off)
}