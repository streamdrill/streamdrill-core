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

import java.io.{DataInputStream, DataOutputStream}
import java.util.Comparator

import streamdrill.io.Serializer
import streamdrill.logging.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


/**
 * Trait for an ExpDecayTrend. Adds update & remove capabilities.
 *
 * @author Mikio L. Braun
 */
trait AbstractExpDecayTrend[T] extends AbstractReadOnlyTrend[T] {
  def update(t: T, timestamp: Long, value: Double)

  def update(t: T, timestamp: Long) {
    update(t, timestamp, 1.0)
  }

  def remove(t: T)
}


/**
 * Entry for an ExpDecayTrend is a counter together with a key.
 *
 * @author Mikio L. Braun
 */
class ExpDecayEntry[T](val key: T, _count: Long, _timestamp: Long) extends ExpDecayCounter(_count, _timestamp) {
  var history: Array[Double] = null

  override def toString = "ExpDecayEntry(%s, %s, %d)".format(key, ExpDecay.dump(count), timestamp)
}

/**
 * ExpDecayTrend
 *
 * Creates a trend of a given maximum capacity where the counters decay with the given half-time (in milli-seconds).
 * The trend will have the given maximum capacity of elements at most and replace older entries to make room based
 * on the ones with the smallest activity.
 *
 * @author Mikio L. Braun
 */
class ExpDecayTrend[T](val capacity: Int, val halfTimeInMs: Long)(implicit ord: Ordering[T])
    extends AbstractExpDecayTrend[T]
    with Logging
    with HasLocks {

  private val eds = new ExpDecaySpace(halfTimeInMs)

  private val counters = new java.util.TreeMap[T, ExpDecayEntry[T]](new ComparatorFct((t1: T, t2: T) => ord.compare(t1, t2)))

  var verbose = false
  var verboseCompare = false
  var iteration = 0

  var replaceOnUpdate = false
  var scoreThreshold = 0.0

  /**
   * Compare function used for the index.
   */
  private val counterCmp = new Comparator[ExpDecayEntry[T]] {
    def compare(e1: ExpDecayEntry[T], e2: ExpDecayEntry[T]): Int = {
      val c = eds.compare(e1, e2)
      val result = if (c != 0) {
        -c
      } else {
        math.signum(ord.compare(e1.key, e2.key))
      }
      if (verboseCompare) {
        info("compare %s and %s => %d".format(e1, e2, result))
      }
      result
    }
  }
  private val index = new java.util.TreeSet[ExpDecayEntry[T]](counterCmp)

  def getCounter(t: T) = counters.get(t)

  def counterIterator = index.iterator.asScala

  /*
   * Event Listeners
   */
  private var eventListeners: List[TrendEventListener[T]] = Nil

  def addEventListener(tel: TrendEventListener[T]) {
    eventListeners ::= tel
  }

  def removeEventListener(tel: TrendEventListener[T]) {
    eventListeners.filterNot(_ == tel)
  }

  def notifyAddEvent(t: T) {
    eventListeners.foreach(_.onAdd(t))
  }

  def notifyRemoveEvent(t: T) {
    eventListeners.foreach(_.onRemove(t))
  }

  /*
   * History stuff
   */
  private var historyLength = 0
  private var historyIndex = 0

  def enableHistory(length: Int) = alone {
    if (length != historyLength) {
      for (c <- counters.values.iterator.asScala) {
        c.history = new Array[Double](length)
      }
      historyLength = length
      historyIndex = 0
    }
  }

  def disableHistory() = alone {
    for (c <- counters.values.iterator.asScala) {
      c.history = null
    }
    historyLength = 0
    historyIndex = 0
  }

  def history(t: T): Array[Double] = together {
    val result = new Array[Double](historyLength)
    val c = getCounter(t)
    if (c != null) {
      for (i <- 0 until historyLength) {
        result(i) = c.history((historyIndex + i) % historyLength)
      }
    }
    result
  }

  def updateHistory() = alone {
    if (historyLength != 0) {
      val t = lastUpdate
      for (c <- counters.values.iterator.asScala) {
        c.history(historyIndex) = eds.scale(c, t)
      }
      historyIndex = (historyIndex + 1) % historyLength
    }
  }

  /**
   * Number of elements in the trend.
   *
   * Always less than or equal to the capacity.
   */
  def size = together {
    counters.size
  }

  /**
   * Time of the last update.
   */
  var lastUpdate = 0L

  private def removeLast() {
    val last = index.last()
    removeEntryFromIndices(last)
    counters.remove(last.key)
  }

  /**
   * Get smallest entry in the trend.
   *
   * @return smallest entry in the trend
   */
  def smallest: T = together {
    index.last().key
  }

  /**
   * Remove an entry from the trend.
   *
   * @param t the element to remove
   */
  def remove(t: T) = alone {
    val c = counters.get(t)
    if (c != null) {
      removeEntry(c)
    }
  }

  /**
   * Called after an entry has been added
   *
   * @param c the entry to be added
   */
  protected def addEntryToIndices(c: ExpDecayEntry[T]) {
    index.add(c)
    indices.foreach(kv => kv._2.addEvent(c))
    notifyAddEvent(c.key)
  }

  protected def removeEntry(e: ExpDecayEntry[T]) {
    removeEntryFromIndices(e)
    counters.remove(e.key)
  }

  /**
   * Called before entry is removed
   *
   * @param c the entry to be removed
   */
  protected def removeEntryFromIndices(c: ExpDecayEntry[T]) {
    notifyRemoveEvent(c.key)
    index.remove(c)
    indices.foreach(_._2.removeEvent(c))
  }

  /**
   * Called after each update.
   */
  protected def postUpdate() {}

  def update(t: T, timestamp: Long, increment: Double) {
    alone {
      iteration += 1
      try {
        val newLastUpdate = if (timestamp > lastUpdate) {
          timestamp
        } else {
          lastUpdate
        }

        val c = counters.get(t)
        if (c == null) {
          while (counters.size >= capacity) {
            removeLast()
          }
          val c = new ExpDecayEntry(t, eds.make(increment), timestamp)
          counters.put(t, c)
          addEntryToIndices(c)
          if (historyLength != 0)
            c.history = new Array[Double](historyLength)
        } else {
          if (!index.contains(c)) {
            println("Hm, how odd. Let's turn on the verbosity and check again... .")
            verboseCompare = true
            index.contains(c)
            verboseCompare = false
          }
          removeEntryFromIndices(c)
          if (!replaceOnUpdate) {
            eds.updateToLater(c, timestamp, increment)
          } else {
            eds.set(c, timestamp, increment)
          }
          addEntryToIndices(c)

          if (counters.size != index.size) {
            error(("number of counters (=%d) and index differ (=%d). Which is very bad!\n" +
                "This happened while processing %s, which is not the problem probably").format(counters.size, index.size, t))
            throw new IllegalStateException("Dataset is corrupt. This shouldn't happen. Please file a bug report.")
          }
        }
        lastUpdate = newLastUpdate

        if (scoreThreshold != 0.0) {
          while (eds.scale(index.last(), lastUpdate) <= scoreThreshold) {
            removeLast()
          }
        }

        postUpdate()
      } catch {
        case ex: NoSuchElementException =>
          throw new NoSuchElementException("when inserting %s: %s".format(t, ex.getMessage))
      }
    }
  }

  /**
   * Return last update for given element
   *
   * @param t the element to be queried
   * @return either timestamp as millieseconds till epoch or -1L if it wasn't in the set.
   */
  def lastUpdate(t: T): Long = together {
    val ti = getCounter(t)
    if (ti == null)
      -1L
    else
      ti.timestamp
  }

  /**
   * Get the score
   *
   * @param t the element to get a score for
   * @param timestamp a timestamp to calculate the score
   * @return the score
   */
  def score(t: T, timestamp: Long): Double = together {
    val c = counters.get(t)
    if (c == null)
      0.0
    else {
      eds.scale(c, timestamp)
    }
  }

  /**
   * Query the trend.
   *
   * @param count how many elements to retrieve
   * @param offset where to start from the top (Warning: actively skips elements!)
   * @return a list of keys.
   */
  def query(count: Int, offset: Int = 0): Seq[T] = together {
    query().drop(offset).take(count).toIndexedSeq
  }

  /**
   * Query the trend, but return an iterator
   *
   * @return
   */
  private def query(): Iterator[T] = index.iterator.asScala.map(_.key)

  /**
   * Query the trend (also return the counters)
   *
   * @param count how many elements to return
   * @param offset the offset of the returned element list
   * @param timestamp a timestamp for the returned counters
   * @return a list of elements together with counters
   */
  def queryWithScore(count: Int, offset: Int = 0, timestamp: Long = 0L): Seq[(T, Double)] = together {
    val ts = if (timestamp == 0L) lastUpdate else timestamp
    val it = index.iterator()
    var o = 0
    var c = 0
    val result = new ArrayBuffer[(T, Double)]()
    while (it.hasNext && c < count) {
      val counter = it.next()
      if (o >= offset) {
        result.append((counter.key, eds.scale(counter, ts)))
        c += 1
      } else {
        o += 1
      }
    }
    result.result()
  }

  def contains(t: T): Boolean = together {
    counters.containsKey(t)
  }

  private var indices: Map[String, SecondaryIndex[ExpDecayEntry[T], _]] = Map()

  var indexNames: Seq[String] = Seq()

  def addIndex[Index](name: String, indexMap: IndexMap[T, Index]) {
    val newIndex = new SecondaryIndex[ExpDecayEntry[T], Index](new IndexMap[ExpDecayEntry[T], Index]() {
      def project(b: ExpDecayEntry[T]): Index = indexMap.project(b.key)

      val comparator: Comparator[Index] = indexMap.comparator
    }, counterCmp)

    alone {
      // Add all the data we already have
      val it = index.iterator
      while (it.hasNext) {
        newIndex.addEvent(it.next())
      }

      // add index
      indices += name -> newIndex
      indexNames = indexNames :+ name
    }
  }

  def removeIndex(name: String) {
    indices -= name
    indexNames = indexNames.filterNot(_ == name)
  }

  def queryIndex[Index](name: String, value: Index, count: Int, offset: Int): Seq[T] = together {
    val i = indices(name).asInstanceOf[SecondaryIndex[ExpDecayEntry[T], Index]]
    i.query(value, count, offset).map(_.key).toIndexedSeq
  }

  def queryIndex[Index](name: String, value: Index): Iterator[T] = together {
    val i = indices(name).asInstanceOf[SecondaryIndex[ExpDecayEntry[T], Index]]
    i.query(value).map(_.key)
  }

  def queryIndexWithScore[Index](name: String, value: Index, count: Int, offset: Int): Seq[(T, Double)] = together {
    val es = queryIndex(name, value, count, offset)
    es.map(e => (e, score(e))).toIndexedSeq
  }

  def save(out: DataOutputStream)(implicit serl: Serializer[T]) = together {
    out.writeLong(lastUpdate)

    if (historyLength != 0) {
      out.writeInt(historyIndex)
    }

    // The actual data
    out.writeInt(counters.size)
    val it = counters.entrySet.iterator()
    while (it.hasNext) {
      val entry = it.next().getValue
      //println("Saving out " + entry)
      out.writeLong(entry.count)
      out.writeLong(entry.timestamp)
      serl.write(out, entry.key)
      if (historyLength != 0) {
        for (i <- 0 until historyLength)
          out.writeDouble(entry.history(i))
      }
    }
  }

  def load(in: DataInputStream)(implicit serl: Serializer[T]): Unit = alone {
    lastUpdate = in.readLong()

    if (historyLength != 0) {
      historyIndex = in.readInt()
    }

    val numEntries = in.readInt()
    counters.clear()

    for (i <- 0 until numEntries) {
      val count = in.readLong()
      val timestamp = in.readLong()
      val key = serl.read(in)

      val c = new ExpDecayEntry(key, count, timestamp)
      //println("Read in " + c)

      if (historyLength != 0) {
        val h = new Array[Double](historyLength)
        for (i <- 0 until historyLength) {
          h(i) = in.readDouble()
        }
        c.history = h
      }

      counters.put(key, c)
      addEntryToIndices(c)
    }
  }

  def serialize(in: DataInputStream)(implicit serl: Serializer[T]) = load(in)

  def serialize(out: DataOutputStream)(implicit serl: Serializer[T]) = save(out)

  def discardEntriesAfter(timestamp: Long) = alone {
    val batchSize = 10000
    var entries = counters.values().asScala.filter(_.timestamp > timestamp).take(batchSize)
    while (entries.nonEmpty) {
      //      debug("Removing batch of %d entries... ".format(entries.size))
      entries.foreach(removeEntry)
      entries = counters.values().asScala.filter(_.timestamp > timestamp).take(batchSize)
    }
  }
}