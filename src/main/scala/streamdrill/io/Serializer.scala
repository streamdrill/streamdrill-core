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

import java.io.{DataInputStream, DataOutputStream}

/**
 * A serializer for different data types.
 *
 * @author Mikio L. Braun
 */
trait Serializer[T] {
  def read(in: DataInputStream): T

  def write(out: DataOutputStream, obj: T)
}

object Serializer {
  implicit val intSeralizer = new Serializer[Int] {
    def read(in: DataInputStream): Int = in.readInt()

    def write(out: DataOutputStream, obj: Int): Unit = out.writeInt(obj)
  }

  implicit val longSeralizer = new Serializer[Long] {
    def read(in: DataInputStream): Long = in.readLong()

    def write(out: DataOutputStream, obj: Long): Unit = out.writeLong(obj)
  }

  implicit val stringSeralizer = new Serializer[String] {
    def read(in: DataInputStream): String = in.readUTF()

    def write(out: DataOutputStream, obj: String): Unit = out.writeUTF(obj)
  }

  implicit val stringSeqSerializer: Serializer[Seq[String]] = new Serializer[Seq[String]] {
    def write(out: DataOutputStream, obj: Seq[String]): Unit = {
      out.writeInt(obj.size)
      obj.foreach(e => out.writeUTF(e))
    }

    def read(in: DataInputStream): Seq[String] = {
      val size = in.readInt()
      var sequence: Seq[String] = Seq()
      0 until size foreach(i => sequence :+= in.readUTF())
      sequence
    }
  }

}
