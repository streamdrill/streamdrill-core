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

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

import scala.math.min


/**
 * Large MMapped file utilities.
 *
 * @author Mikio L. Braun
 */
class LargePosition(bufferSize: Int) {
  private var pos = 0L
  private var bufferIndex = 0
  private var bufferPos = 0

  /**
   * got to position p
   */
  def position_=(p: Long) {
    val bi = p / bufferSize
    val bp = p - bi * bufferSize
    bufferIndex = bi.toInt
    bufferPos = bp.toInt
    pos = p
  }

  /**
   * Increment position
   * @param amt how many bytes
   */
  def incPos(amt: Int) {
    bufferPos += amt
    pos += amt
    if (bufferPos >= bufferSize) {
      bufferPos -= bufferSize
      bufferIndex += 1
    }
  }

  /**
   * Get full position
   * @return
   */
  def position = pos

  /**
   * get buffer index
   */
  def index = bufferIndex

  /**
   * get offset within a buffer
   */
  def offset = bufferPos
}

class LargeMMappedFile(fc: FileChannel, bufferSize: Int = Integer.MAX_VALUE) extends LargeByteBuffer {
  private val totalSize = fc.size()
  private val numBuffers = ((totalSize + bufferSize - 1) / bufferSize).toInt

  private val buffers = (0L until numBuffers).map(i =>
    LargeMMappedFile.map(fc, i * bufferSize, min(bufferSize, totalSize - i * bufferSize))).toArray

  fc.close()

  val pos = new ThreadLocal[LargePosition]() {
    override protected def initialValue() = new LargePosition(bufferSize)
  }

  def position(p: Long) {
    pos.get.position = p
  }

  def position() = pos.get.position

  /**
   * Increment internal position by the given amount.
   *
   * Does not reposition the position in the buffer.
   */
  private def incPos(amt: Int) {
    pos.get.incPos(amt)
  }

  def read(out: Array[Byte], idx: Int, len: Int): Int = {
    //println("Reading %d bytes at (%d/%d).".format(len, bufferIndex, bufferPos))
    val p = pos.get
    val bytesAvailable = min(bufferSize - p.offset, len)
    buffers(p.index).synchronized {
      buffers(p.index).position(p.offset)
      buffers(p.index).get(out, idx, bytesAvailable)
    }
    p.incPos(bytesAvailable)
    bytesAvailable
  }

  def get(out: Array[Byte], idx: Int, len: Int) {
    var i = idx
    while (i < len) {
      i += read(out, i, len - i)
    }
  }

  def remaining(): Long = totalSize - position()

  def getByte: Byte = {
    val p = pos.get
    val result = buffers(p.index).get(p.offset)
    incPos(1)
    result
  }

  private def getByteAsInt: Int = {
    val i = getByte.toInt
    if (i < 0) 256 + i else i
  }

  private def getByteAsLong: Long = {
    val l = getByte.toLong
    if (l < 0) 256 + l else l
  }

  def getShort: Short = (getByteAsInt << 8 | getByteAsInt).toShort

  def getInt: Int = getByteAsInt << 24 | getByteAsInt << 16 | getByteAsInt << 8 | getByteAsInt

  def getLong: Long = getByteAsLong << 56 | getByteAsLong << 48 | getByteAsLong << 40 | getByteAsLong << 32 | getByteAsLong << 24 | getByteAsLong << 16 | getByteAsLong << 8 | getByteAsLong

  /*def ubyteToLong(b: Byte): Long = {
    if (b < 0) 256 + b.toLong else b.toLong
  }

  val longBuf = new Array[Byte](8)
  def getLong(): Long = {
    get(longBuf)
    ubyteToLong(longBuf(0)) << 56 | ubyteToLong(longBuf(1)) << 48 | ubyteToLong(longBuf(2)) << 40 | ubyteToLong(longBuf(3)) << 32 |
    ubyteToLong(longBuf(4)) << 24 | ubyteToLong(longBuf(5)) << 16 | ubyteToLong(longBuf(6)) << 8  | ubyteToLong(longBuf(7))
  }*/


  def getFloat: Float = java.lang.Float.intBitsToFloat(getInt)

  def getDouble: Double = java.lang.Double.longBitsToDouble(getLong)

  def size(): Long = totalSize
}

object LargeMMappedFile {
  var cachedMaps = Map[(String, Long, Long), MappedByteBuffer]()

  def map(ch: FileChannel, pos: Long, len: Long): MappedByteBuffer = {
    val key = (ch.toString, pos, len)

    val m = cachedMaps.get(key)

    if (m.isDefined) {
      m.get
    } else {
      val nm = ch.map(FileChannel.MapMode.READ_ONLY, pos, len)
      cachedMaps += key -> nm
      nm
    }
  }
}