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

import java.util.Random

import org.junit.Assert._
import org.junit._
import streamdrill.logging.Logging

class ExpDecayTrendTest extends Logging {
  def compareHistories(a: Seq[Double], b: Array[Double]): Boolean = {
    if (a.length != b.length)
      false
    else {
      for (i <- 0 until a.length) {
        if (math.abs(a(i) - b(i)) > 1e-3) {
          return false
        }
      }
      true
    }
  }

  private val EPS = 1e-10

  @Test
  def testUpdatesForOne() {
    val t = new ExpDecayTrend[String](10, 1000)

    t.update("foo", 1000L, 1.0)
    assertEquals(1.0, t.score("foo", 1000L), EPS)
    assertEquals(0.5, t.score("foo", 2000L), EPS)

    t.update("foo", 1000L, 1.0)
    assertEquals(2.0, t.score("foo", 1000L), EPS)

    t.update("foo", 2000L, 1.0)
    assertEquals(2.0, t.score("foo", 2000L), EPS)
    assertEquals(4.0, t.score("foo", 1000L), EPS)
  }

  @Test
  def keyOrdering() {
    val t = new ExpDecayTrend[String](10, 1000)

    t.update("a", 1000L, 1.0)
    t.update("c", 1000L, 1.0)
    t.update("b", 1000L, 1.0)

    assertTrue(Seq("a", "b", "c") == t.query(10))
  }

  @Test
  def testUpdatesMultiple() {
    val t = new ExpDecayTrend[String](10, 1000)

    debug("adding a")
    t.update("a", 1000L, 1.0)
    debug("adding b")
    t.update("b", 2000L, 1.0)
    debug("adding c")
    t.update("c", 3000L, 1.0)
    assertEquals(3, t.size)

    debug(t.queryWithScore(10))

    val q = t.query(10)
    assertEquals(3, q.size)
    assertEquals("c", q(0))
    assertEquals(1.0, t.score(q(0), 3000L), EPS)
    assertEquals("b", q(1))
    assertEquals(0.5, t.score(q(1), 3000L), EPS)
    assertEquals("a", q(2))
    assertEquals(0.25, t.score(q(2), 3000L), EPS)
  }

  private def getTrend[T](t: ExpDecayTrend[T]) = t.query(t.size).map(s => (s, t.score(s)))

  private def assertEqualTrend(expected: Seq[(String, Double)], actual: Seq[(String, Double)]) {
    if (expected.size != actual.size ||
        expected.zip(actual).exists(ea => ea._1._1 != ea._2._1 || math.abs(ea._1._2 - ea._2._2) > EPS)) {
      throw new AssertionError("\nExpected :%s\nActual   :%s".format(expected, actual))
    }
  }

  @Test
  def testUpdateWithReplace() {
    val t = new ExpDecayTrend[String](4, 1000L)

    debug("a @ 1s")
    t.update("a", 1000L, 1.0)
    assertEquals(Seq(("a", 1.0)), getTrend(t))
    debug(getTrend(t))

    debug("b @ 2s")
    t.update("b", 2000L, 1.0)
    assertEqualTrend(Seq(("b", 1.0), ("a", 0.5)), getTrend(t))
    debug(getTrend(t))

    debug("c @ 3s")
    t.update("c", 3000L, 1.0)
    assertEqualTrend(Seq("c" -> 1.0, "b" -> 0.5, "a" -> 0.25), getTrend(t))
    debug(getTrend(t))

    //t.verbose = true
    //t.verboseCompare = true
    debug("a @ 4s")
    t.update("a", 4000L, 1.0)
    assertEqualTrend(Seq("a" -> 1.125, "c" -> 0.5, "b" -> 0.25), getTrend(t))
    debug(getTrend(t))

    debug("d @ 5s")
    t.update("d", 5000L, 1.0)
    assertEqualTrend(Seq("d" -> 1.0, "a" -> 0.5625, "c" -> 0.25, "b" -> 0.125), getTrend(t))
    debug(getTrend(t))

    // ok, this should lead to the least one being expunged.
    debug("e @ 7s")
    t.update("e", 6000L, 1.0)
    assertEqualTrend(Seq("e" -> 1.0, "d" -> 0.5, "a" -> 0.28125, "c" -> 0.125), getTrend(t))
    debug(getTrend(t))
  }

  /*@Test
  def testFactor() {
    val t = new ExpDecayTrend[String](10, 1000)

    assertEquals(0.5, ExpDecay.toDouble(ExpDecay.makeExp(t.factor(1000L, 2000L))), EPS)

    assertEquals(2.0, ExpDecay.toDouble(ExpDecay.makeExp(t.factor(2000L, 1000L))), EPS)

    assertEquals(1.0, ExpDecay.toDouble(ExpDecay.makeExp(t.factor(2000L, 2000L))), EPS)
  }*/

  private def runSome(t: ExpDecayTrend[Int], n: Int) {
    val saved = System.nanoTime
    var c = 0
    while (c < n) {
      val r = scala.util.Random.nextInt(10000)
      //debug("adding %d".format(r))
      t.update(r, c * 100L, 1.0)
      c += 1
    }
    val elapsed = (System.nanoTime - saved) / 1e9
    debug("%d iters in %.3fs (%.1f per sec)".format(n, elapsed, n / elapsed))
  }

  //@Test
  def bench() {
    val n = 1000000

    val t = new ExpDecayTrend[Int](1000, 1000)

    for (k <- 1 to 100) {
      runSome(t, n)
    }
  }

  //@Test
  def fillRandom() {
    val halftime = 3600000L
    val trend = new ExpDecayTrend[Int](1000, halftime)

    val r = new Random(123456L)

    var t = 0
    for (i <- 1 to 1000) {
      val x = r.nextInt(1000)
      val d = r.nextInt(1000)
      t += d
      trend.update(x, t)
    }

    for ((i, s) <- trend.queryWithScore(1000)) {
      debug("%10d %f".format(i, s))
    }

    val c570 = trend.getCounter(570)
    val c65 = trend.getCounter(65)
    val c967 = trend.getCounter(967)
    debug("score for 570 = " + trend.score(570))
    debug("score for 65  = " + trend.score(65))
    debug("score for 967 = " + trend.score(967))
    val eds = new ExpDecaySpace(halftime)
    debug(eds.compare(c570, c65))
    debug(eds.compare(c65, c967))
    val l = trend.lastUpdate
    debug("c65 =  %s timediff to current %d, bare score = %f".format(c65, l - c65.timestamp, ExpDecay.toDouble(c65.count)))
    debug("c967 = %s timediff to current %d, bare score = %f".format(c967, l - c967.timestamp, ExpDecay.toDouble(c967.count)))

    debug("c65 = %xL, %d".format(c65.count, c65.timestamp))
    debug("c967 = %xL, %d".format(c967.count, c967.timestamp))
    debug("last update = %d".format(l))
  }

  @Test
  def updateWithReplace() {
    val trend = new ExpDecayTrend[String](1000, 1000L)
    trend.replaceOnUpdate = true

    trend.update("1", 1000L)
    assertEqualTrend(Seq("1" -> 1.0), getTrend(trend))
    trend.update("2", 2000L)
    assertEqualTrend(Seq("2" -> 1.0, "1" -> 0.5), getTrend(trend))
    trend.update("3", 3000L)
    assertEqualTrend(Seq("3" -> 1.0, "2" -> 0.5, "1" -> 0.25), getTrend(trend))
    trend.update("1", 4000L)
    assertEqualTrend(Seq("1" -> 1.0, "3" -> 0.5, "2" -> 0.25), getTrend(trend))
    trend.update("3", 5000L)
    assertEqualTrend(Seq("3" -> 1.0, "1" -> 0.5, "2" -> 0.125), getTrend(trend))
  }

  @Test
  def updateWithThreshold() {
    val trend = new ExpDecayTrend[String](1000, 1000L)
    trend.scoreThreshold = 0.3
    trend.update("1", 1000L)
    assertEqualTrend(Seq("1" -> 1.0), getTrend(trend))
    trend.update("2", 2000L)
    assertEqualTrend(Seq("2" -> 1.0, "1" -> 0.5), getTrend(trend))
    trend.update("3", 3000L)
    assertEqualTrend(Seq("3" -> 1.0, "2" -> 0.5), getTrend(trend))
    trend.update("2", 4000L)
    assertEqualTrend(Seq("2" -> 1.25, "3" -> 0.5), getTrend(trend))
  }

  @Test
  def discardEntriesAfter() {
    val trend = new ExpDecayTrend[String](1000, 1000L)
    trend.update("1", 1000L)
    trend.update("2", 2000L)
    trend.update("3", 3000L)
    trend.update("2", 4000L)
    assertEqualTrend(Seq("2" -> 1.25, "3" -> 0.5, "1" -> 0.125), getTrend(trend))

    trend.lastUpdate = 3000L
    trend.discardEntriesAfter(3000L)
    assertEqualTrend(Seq("3" -> 1.0, "1" -> 0.25), getTrend(trend))

    trend.lastUpdate = 2000L
    trend.discardEntriesAfter(2000L)
    assertEqualTrend(Seq("1" -> 0.5), getTrend(trend))
  }

  @Test
  def discardEntriesAfterLarge() {
    val n = 100000
    val trend = new ExpDecayTrend[Int](2*n, 1000L)
    for (i <- 1 to n) {
      trend.update(i, i * 10L)
    }
    assertEquals(n, trend.size)

    trend.discardEntriesAfter(10 * n - n)
    assertEquals(n - n / 10, trend.size)

    val t = n - n / 10
    assertEquals(Seq(t, t-1, t-2, t-3, t-4, t-5, t-6, t-7, t-8, t-9), trend.query(10))
  }

  @Test
  def historyTest() {
    val n = 1000
    val trend = new ExpDecayTrend[Int](n, 1000L)
    trend.enableHistory(5)

    trend.update(1, 0L)

    assertTrue(compareHistories(Seq(0.0, 0.0, 0.0, 0.0, 0.0), trend.history(1)))

    trend.lastUpdate = 1000L
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.0, 0.0, 0.0, 0.0, 0.5), trend.history(1)))

    trend.lastUpdate = 2000L
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.0, 0.0, 0.0, 0.5, 0.25), trend.history(1)))

    trend.update(1, 3000L)
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.0, 0.0, 0.5, 0.25, 1.125), trend.history(1)))

    trend.lastUpdate = 4000L
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.0, 0.5, 0.25, 1.125, 0.5625), trend.history(1)))

    trend.lastUpdate = 5000L
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.5, 0.25, 1.125, 0.5625, 0.28125), trend.history(1)))

    trend.lastUpdate = 6000L
    trend.updateHistory()
    assertTrue(compareHistories(Seq(0.25, 1.125, 0.5625, 0.28125, 0.140625), trend.history(1)))
  }
}
