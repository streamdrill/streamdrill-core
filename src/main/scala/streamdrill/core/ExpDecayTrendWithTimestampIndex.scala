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

package streamdrill.core

import streamdrill.logging.Logging
import streamdrill.util.Compares

import scala.collection.JavaConverters._

/**
 * Exponential Decay Trend combined with a timestamp index.
 *
 * @author Mikio L. Braun
 */
class ExpDecayTrendWithTimestampIndex[T](_capacity: Int, _halftimeInMs: Long)(implicit ord: Ordering[T])
    extends ExpDecayTrend[T](_capacity, _halftimeInMs) with Logging {
  var maxTimeToLive = 0L

  private val timestampIndex = new java.util.TreeSet[ExpDecayEntry[T]](new java.util.Comparator[ExpDecayEntry[T]] {
    def compare(o1: ExpDecayEntry[T], o2: ExpDecayEntry[T]): Int =
      Compares.combine(Compares.compare(o1.timestamp, o2.timestamp), ord.compare(o1.key, o2.key))
  })

  override protected def addEntryToIndices(e: ExpDecayEntry[T]) {
    super.addEntryToIndices(e)
    timestampIndex.add(e)
  }

  override protected def removeEntryFromIndices(e: ExpDecayEntry[T]) {
    super.removeEntryFromIndices(e)
    timestampIndex.remove(e)
  }

  override protected def postUpdate() {
    if (maxTimeToLive != 0L) {
      while (!timestampIndex.isEmpty && lastUpdate - timestampIndex.first.timestamp >= maxTimeToLive) {
        //info("removing " + timestampIndex.first)
        removeEntry(timestampIndex.first)
      }
    }
  }

  def dump() {
    for ((e, i) <- timestampIndex.asScala.toSeq.zipWithIndex) {
      println("%d: %s".format(i, e))
    }
  }
}
