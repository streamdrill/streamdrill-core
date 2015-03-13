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

class SecondaryIndexTest extends Logging {
  @Test
  def test2ndaryIndices() {
    val t = new ExpDecayTrend[String](100, 1000)
    t.addIndex("first letter", IndexMap((i: String) => i.substring(0, 1)))

    t.update("10", 1000)
    t.update("11", 2000)
    t.update("20", 3000)

    assertEquals(List("11", "10"), t.queryIndex("first letter", "1", 10, 0).toList)
    assertEquals(List("20"), t.queryIndex("first letter", "2", 10, 0).toList)
    assertEquals(List(), t.queryIndex("first letter", "3", 10, 0).toList)
  }

  @Test
  def test2ndaryTestEvents() {
    val t = new ExpDecayTrend[(String,String)](100, 1000)
    t.addIndex("0", IndexMap((e: (String, String)) => e._1))
    t.addIndex("1", IndexMap((e: (String, String)) => e._2))

    val users = Seq("frank", "paul", "leo", "felix", "jan")
    val actions = Seq("wins", "looses", "ties")
    val r = new Random()

    for (i <- 1 to 100) {
      val u = users(r.nextInt(users.length))
      val a = actions(r.nextInt(actions.length))
      t.update((u,a), i * 100)
    }

    debug(t.queryWithScore(1000, 0))

    debug(t.queryIndexWithScore("0", "frank", 10, 0))
  }
}
