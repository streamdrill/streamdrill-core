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


/**
 * Exponential Decay Space
 *
 * A space where counters with timestamps decay over time. Extended over
 * all of time, each point is actually a line in this space where
 * the slope is identical and given by the half-time.
 *
 * @author Mikio L. Braun
 */
class ExpDecaySpace(halfTimeInMs: Long, suggestedResolutionInMs: Long = 0) extends Logging {
  // number of post-comma digits used in comparison
  private val compareBits = 22

  val maxTicksPerHalfTime = 1L << (64 - compareBits - (ExpDecay.maxBits - ExpDecay.base))
  /**
   * Resolution: how far can events be apart.
   */
  val resolutionInMs = if (suggestedResolutionInMs == 0) {
    if (halfTimeInMs <= 60000L) // 1 minute -> 1ms (60000L ticks per half time t/ht)
      1
    else if (halfTimeInMs <= 86400000L) // a day -> 1s, (86.4k t/ht)
      1000L
    else if (halfTimeInMs <= 2592000000L) // 30 days -> 10s, (86.4k t/ht)
      30000L
    else if (halfTimeInMs <= 31536000000L) // a year -> 1m (105.1k t/ht)
      300000L
    else // more than a year -> 10m
      600000L
  } else {
    suggestedResolutionInMs
  }

  if (halfTimeInMs % resolutionInMs != 0) {
    warn("Halftime should be multiply of resolution in that bracket!")
  }

  val ticksPerHalfTime = halfTimeInMs / resolutionInMs
  if (ticksPerHalfTime >= maxTicksPerHalfTime) {
    throw new IllegalArgumentException("too many ticks per half time, might lead to numerical problems.")
  }

  val shiftBits = math.ceil(math.log(halfTimeInMs / resolutionInMs) / math.log(2)).toInt + 1

  override def toString = "ExpDecaySpace(halftime %d, resolution %d, shift bits %d)".format(halfTimeInMs, resolutionInMs, shiftBits)

  val maxTimeDistanceInTicks = java.lang.Long.MAX_VALUE >> compareBits
  val maxTimeDistance = maxTimeDistanceInTicks * resolutionInMs

  def make(value: Double) = ExpDecay.make(value)

  /*
  // original compare function
  def compare(e1: ExpDecayCounter, e2: ExpDecayCounter): Int = {
    val shift = factor(e1.timestamp, e2.timestamp)
    ExpDecay.cmpWithShift(e1.count, e2.count, shift)
  }
  }*/

  def time2ticks(t: Long) = t / resolutionInMs

  /**
   * Compare two ExpDecayCounters, rescaling to account for timing differences.
   *
   * In this implementation, a number of bits are discarded, so it can be that numbers
   * are slightly out of order in the last post-comma digits.
   *
   * More limits:
   * - time difference in ticks may not be larger than 2**32
   */
  def compare(e1: ExpDecayCounter, e2: ExpDecayCounter): Int = {
    val t1 = time2ticks(e1.timestamp)
    val t2 = time2ticks(e2.timestamp)
    val dt = t1 - t2
    if (dt < -maxTimeDistanceInTicks || dt > maxTimeDistanceInTicks) {
      error("numeric overflow in compare operation because time distances are too far apart.")
    }
    val sf = halfTimeInMs / resolutionInMs

    val cshift = ExpDecay.base - compareBits
    val sc = (ExpDecay.count(e2.count) >> cshift) - (ExpDecay.count(e1.count) >> cshift)
    if (sc * sf / sf != sc) {
      //println("sc * sf yields overflow!!!")
      //println("e1 = %s, e2 = %s, time diff in ticks = %d, count diff = %d, halftime in ticks mult = %d (prod = %d)".format(e1, e2, dt >> ExpDecay.base, sc, sf, sc * sf))
      //throw new Exception("Ouch")
      error("numeric overflow in compare operation because count differences and ticks per half time are too large.")
    }

    val result = cmpLong(dt << compareBits, sc * sf)
    //println("e1 = %s, e2 = %s, time diff in ticks = %d, count diff = %d, halftime in ticks mult = %d (prod = %d) => result = %d".format(e1, e2, dt, sc, sf, sc * sf, result))
    result
  }

  /*def cmpLongWithShiftAndMult(l: Long, ls: Int, r: Long, a: Int) {
    // compure r * a
    val r0 = a * hi(l)
    val r1 = a * lo(l)
  }

  private def hi(l: Long): Long = (l & 0xffffFFFF00000000L) >> 32

  private def lo(l: Long): Long = l & 0xffffFFFFL
  */

  def cmpLong(l: Long, r: Long): Int = {
    if (l < r) -1 else if (l > r) 1 else 0
  }

  /**
   * Get the double value of an ExpDecayCounter at a given time stamp
   */
  def scale(c: ExpDecayCounter, timestamp: Long): Double = ExpDecay.toDouble(rescale(c, timestamp))

  /**
   * Rescale the counter of an ExpDecayCounter
   */
  def rescale(c: ExpDecayCounter, timestamp: Long): Long = ExpDecay.mult(ExpDecay.makeExp(factor(c.timestamp, timestamp)), c.count)

  /**
   * Factor to scale an ExpDecayCounter to the given data point
   * @param t1 time stamp
   * @param t2 time stamp
   * @return difference as Double
   */
  def factor(t1: Long, t2: Long): Double = (t1 - t2).toDouble / halfTimeInMs

  /**
   * Update a counter by adding a point
   *
   * @param c a point in the ExpDecaySpace
   * @param timestamp timestamp of the value to add
   * @param value value to add
   * @return the counter for the updated ExpDecayCounter at the given timestamp time.
   */
  def update(c: ExpDecayCounter, timestamp: Long, value: Double): Long = ExpDecay.add(rescale(c, timestamp), value)

  def updateToLater(c: ExpDecayCounter, timestamp: Long, value: Double) {
    if (timestamp > c.timestamp) {
      c.count = update(c, timestamp, value)
      c.timestamp = timestamp
    } else {
      val c2 = new ExpDecayCounter(ExpDecay.make(value), timestamp)
      c.count = ExpDecay.add(rescale(c2, c.timestamp), ExpDecay.toDouble(c.count))
    }
  }

  def toDouble(c: ExpDecayCounter): Double = ExpDecay.toDouble(c.count)

  def toDouble(c: ExpDecayCounter, timestamp: Long): Double = scale(c, timestamp)

  def set(c: ExpDecayCounter, timestamp: Long, value: Double): ExpDecayCounter = {
    c.count = make(value)
    c.timestamp = timestamp
    c
  }
}
