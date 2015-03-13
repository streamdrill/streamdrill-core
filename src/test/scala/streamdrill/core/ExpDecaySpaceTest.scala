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

import org.junit.Assert._
import org.junit._
import streamdrill.logging.Logging

class ExpDecaySpaceTest extends Logging {
  val eps = 1e-4

  @Test
  def updateToLater() {
    val eds = new ExpDecaySpace(1000L)

    // Initialize to 0 (count), 0 (timestamp)
    val c = new ExpDecayCounter(0, 0L)
    assertEquals(0.0, eds.toDouble(c), eps)
    assertEquals(0L, c.timestamp)

    // Add 1 at 0
    eds.updateToLater(c, 0L, 1.0)
    assertEquals(1.0, eds.toDouble(c), eps)
    assertEquals(0L, c.timestamp)

    // Add 1 at 1000
    eds.updateToLater(c, 1000L, 1.0)
    assertEquals(1.5, eds.toDouble(c), eps)
    assertEquals(1000L, c.timestamp)

    // Add 1 at 0 (but timestamp should stay at 0)
    eds.updateToLater(c, 0L, 1.0)
    assertEquals(2.0, eds.toDouble(c), eps)
    assertEquals(1000L, c.timestamp)
  }

  @Test
  def largeNumbers() {
    for (ht <- Seq(1L, 1000L, 60000L, 86400000L, 2592000000L, 31536000000L, 31536000000L * 10)) {
      val eds = new ExpDecaySpace(ht)
      debug("Halftime: " + ht)
      debug("shift: " + eds.shiftBits)
      debug("Resolution: " + eds.resolutionInMs)
      debug("Ticks Per Half-time: " + eds.ticksPerHalfTime)
      debug("Max. TPHT: " + eds.maxTicksPerHalfTime)
      debug("Maximal time distance = %d (or about %d days, %d years)".format(eds.maxTimeDistance,
        eds.maxTimeDistance / (1000L * 3600 * 24),
        eds.maxTimeDistance / (1000L * 3600 * 24 * 365)))

      val c1 = new ExpDecayCounter(eds.make(0x7fffffff), 0L)
      val c2 = new ExpDecayCounter(eds.make(0x7fffffff), eds.maxTimeDistance)
      val c3 = new ExpDecayCounter(eds.make(0x7fffffff), 1L)

      debug(c1)
      debug(c2)
      debug(c3)

      assertEquals(0, eds.compare(c1, c1))
      assertEquals(0, eds.compare(c2, c2))
      assertEquals(-1, eds.compare(c1, c2))
      assertEquals(1, eds.compare(c2, c1))
      assertEquals(if (eds.resolutionInMs <= 1L) -1 else 0, eds.compare(c1, c3))
      assertEquals(-1, eds.compare(c3, c2))

      debug("-----")
    }
  }

  @Test
  def testShiftOddities() {
    val eds = new ExpDecaySpace(1000L)
    val a = new ExpDecayCounter(eds.make(1.0), 1000L)
    eds.updateToLater(a, 4000L, 1.0)
    //println(a)
    //println(ExpDecay.toDouble(eds.rescale(a, 5000L)))

    val d = new ExpDecayCounter(eds.make(1.0), 5000L)
    //println(d)
    //println(ExpDecay.toDouble(eds.rescale(d, 5000L)))

    assertEquals(-1, eds.compare(a, d))
    assertEquals(1, eds.compare(d, a))
  }

  @Test
  def compare() {
    val eds = new ExpDecaySpace(3600000L)

    val a = new ExpDecayCounter(eds.make(2.0), 0L)
    val b = new ExpDecayCounter(eds.make(1.0), 3600000L)
    assertEquals(0, eds.compare(a, b))

    val x = new ExpDecayCounter(0x400077e20c51dcceL, 482054L)
    val y = new ExpDecayCounter(0x4000c311864d1532L, 478052L)
    val t = 483062
    //println("x = %f (scaled to t %f)".format(eds.toDouble(x), eds.toDouble(x, t)))
    //println("y = %f (scaled to t %f)".format(eds.toDouble(y), eds.toDouble(y, t)))
    assertEquals(-1, eds.compare(x, y))
    assertEquals(1, eds.compare(y, x))
  }

  def signToCmp(s: Int) = if (s < 0) "<" else if (s > 0) ">" else "="

  @Test
  def serienError() {
    val eds = new ExpDecaySpace(3600000L)
    debug(eds)
    val base = 1387234832000L
    val a = new ExpDecayCounter(0x4000ca21268892a2L, 1387234843000L - base)
    val b = new ExpDecayCounter(0x4000ca4e933c06d8L, 1387234838000L - base)
    val c = new ExpDecayCounter(0x4000ca851c34d115L, 1387234832000L - base)
    debug("a = %s".format(a))
    debug("b = %s".format(b))
    debug("c = %s".format(c))
    debug("a %s b".format(signToCmp(eds.compare(a, b))))
    debug("b %s c".format(signToCmp(eds.compare(b, c))))
    debug("a %s c".format(signToCmp(eds.compare(a, c))))
  }
}
