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

import java.util.Comparator

import streamdrill.logging.Logging

import scala.collection.JavaConverters._

/**
 * Secondary Index
 *
 * Secondary index to add to some base entries.
 *
 * You need a map which extracts the index (just one entry), and a comparator for
 * the original ordering.
 *
 * @author Mikio L. Braun
 */
class SecondaryIndex[Base,Index](indexMap: IndexMap[Base,Index], cmp: Comparator[Base]) extends Logging {
  private val index = new java.util.TreeMap[Index, java.util.TreeSet[Base]](indexMap.comparator)

  /**
   * Add an event to the secondary index
   *
   * @param t basic entry which is mapped to the index entry
   */
  def addEvent(t: Base) {
    val s = indexMap.project(t)
    val ts = index.get(s)
    if (ts == null) {
      val newts = new java.util.TreeSet[Base](cmp)
      newts.add(t)
      index.put(s, newts)
    } else {
      ts.add(t)
    }
  }

  /**
   * Remove an event from the secondary index
   *
   * @param t basic entry to be removed
   */
  def removeEvent(t: Base) {
    val s = indexMap.project(t)
    val ts = index.get(s)
    if (ts != null) {
      ts.remove(t)
      if (ts.isEmpty) {
        index.remove(s)
      }
    } else {
      warn("no index entry for %s (mapping to %s)".format(t, s))
    }
  }

  /**
   * Run a query
   *
   * Basically, it looks up the index and then does a query on the secondary index.
   *
   * @param i index value to query
   * @param count number of elements to reqtrieve
   * @param offset offset from beginning
   * @return sequence of base entries
   */
  def query(i: Index, count: Int, offset: Int): Seq[Base] = {
    query(i).drop(offset).take(count).toIndexedSeq
  }

  def query(i: Index): Iterator[Base] = {
    val ts = index.get(i)
    if (ts == null) {
      Seq().iterator
    } else {
      ts.iterator.asScala
    }
  }
}