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
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.Charset

/**
 * RandomAccesWriter
 *
 * Wrapper for a file which supports writing a number of primitive values
 * and seeking.
 *
 * @author Mikio L. Braun
 */
trait RandomAccessWriter {
  def write(b: Array[Byte]) { write(b, 0, b.length) }
  def write(b: Array[Byte], off: Int, len: Int)
  def writeByte(b: Int)
  def writeShort(v: Int)
  def writeInt(v: Int)
  def writeLong(v: Long)
  def writeFloat(f: Float)
  def writeDouble(d: Double)
  def writeUTF(s: String)
  def getFilePointer: Long
  def seek(p: Long)
  def close()
  def flushBuffer()
}

/**
 * BufferedRandomAccessFile based on a file channel.
 *
 * @author Mikio L. Braun
 */
class BufferedRandomAccessFile(chan: FileChannel, bufferSize: Int) extends RandomAccessWriter {
  private val utf8 = Charset.forName("UTF-8")
  private val buffer = ByteBuffer.allocate(bufferSize)
  private var position = 0L
  //buffer.order(ByteOrder.nativeOrder())

  def flushBuffer() {
    val p = buffer.position()
    buffer.flip()
    val bytesWritten = chan.write(buffer)
    assert(bytesWritten == p)
    buffer.clear()
    position += p
  }

  def write(b: Array[Byte], off: Int, len: Int) {
    var i = off
    var r = len
    while (r > 0) {
      val l = buffer.remaining
      if (l < r) {
        buffer.put(b, i, l)
        flushBuffer()
        i += l
        r -= l
      } else {
        buffer.put(b, i, r)
        r = 0
      }
    }
  }

  def writeByte(v: Int) {
    if (buffer.remaining < 1)
      flushBuffer()
    buffer.put(v.toByte)
  }

  def writeShort(v: Int) {
    if (buffer.remaining < 2)
      flushBuffer()
    buffer.putShort(v.toShort)
  }

  def writeInt(v: Int) {
    if (buffer.remaining < 4)
      flushBuffer()
    buffer.putInt(v)
  }

  def writeLong(v: Long) {
    if (buffer.remaining < 8)
      flushBuffer()
    buffer.putLong(v)
  }

  def writeFloat(v: Float) {
    if (buffer.remaining < 4)
      flushBuffer()
    buffer.putFloat(v)
  }

  def writeDouble(v: Double) {
    if (buffer.remaining < 8)
      flushBuffer()
    buffer.putDouble(v)
  }

  def writeUTF(s: String) {
    write(s.getBytes(utf8))
  }

  def getFilePointer: Long = {
    position + buffer.position()
  }

  def seek(pos: Long) {
    flushBuffer()
    chan.position(pos)
    position = pos
  }

  def close() {
    flushBuffer()
    chan.close()
  }
}

object BufferedRandomAccessFile {
  def apply(raf: RandomAccessFile, buffer: Int): BufferedRandomAccessFile = new BufferedRandomAccessFile(raf.getChannel, buffer)

  def apply(file: File, buffer: Int): BufferedRandomAccessFile = apply(new RandomAccessFile(file, "rw"), buffer)
}