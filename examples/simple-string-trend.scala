/*
 * Simple example to create a trend and put some data in.
 */

import streamdrill.core.ExpDecayTrend

import scala.util.Random

// Create a trend with a one second half time
val t = new ExpDecayTrend[String](1000, 1000L)

val r = new Random()

// push a million points from a slightly skewed distribution between 0 and 10000
// spaced one millisecond apart
for (i <- 1 to 1000000) {
  val n = r.nextDouble() * 100
  val s = (n * n).toInt.toString
  t.update(s, i)
}

// Top ten entries:
for ((element, score) <- t.queryWithScore(10)) {
  println("%10s: %.2f".format(element, score))
}