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

package streamdrill.util

/**
 * Helper function to parse time scale strings.
 *
 * @author Mikio L. Braun
 */
object TimescaleParser {
  val NumericTimescale = "([0-9]+)([wdhms])".r
  val PlainInteger ="([0-9]+)".r

  val SECOND = 1000L
  val MINUTE = 60L*1000
  val HOUR = 3600L*1000
  val DAY = 24L*3600*1000
  val WEEK = 7L*24*3600*1000

  val durationInMs = Map(
    "s" -> SECOND,
    "m" -> MINUTE,
    "h" -> HOUR,
    "d" -> DAY,
    "w" -> WEEK
  )

  def parseTimescale(s: String): Long = s match {
    case "week" => durationInMs("w")
    case "day" => durationInMs("d")
    case "hour" => durationInMs("h")
    case "minute" => durationInMs("m")
    case "second" => durationInMs("s")
    case NumericTimescale(num, dur) => num.toLong * durationInMs(dur)
    case PlainInteger(num) => num.toLong
    case _ => throw new IllegalArgumentException("Cannot parse Timescale %s".format(s))
  }

  def formatTimescales(ms: Long): String = if (ms % WEEK == 0) {
    if (ms == WEEK) "week" else (ms / WEEK).toString + "w"
  } else if (ms % DAY == 0) {
    if (ms == DAY) "day" else (ms / DAY).toString + "d"
  } else if (ms % HOUR == 0) {
    if (ms == HOUR) "hour" else (ms / HOUR).toString + "h"
  } else if (ms % MINUTE == 0) {
    if (ms == MINUTE) "minute" else (ms / MINUTE).toString + "m"
  } else if (ms % SECOND == 0) {
    if (ms == SECOND) "second" else (ms / SECOND).toString + "s"
  } else {
    ms.toString
  }
}
