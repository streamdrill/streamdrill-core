/*
 * Simple example of using secondary indices
 *
 * Essentially, you need to have an implicit ordering for
 * your entry class, and then define secondary indices by
 * specifying maps which extract the value you want to
 * compute the indices on.
 *
 * These IndexMaps must be constant, i.e., always return the
 * same value for the same class.
 */

import streamdrill.core.{IndexMap, ExpDecayTrend}

// Simple class to represent peoples that come from certain cities
class Entry(val name: String, val city: String) {
  override def toString = "(" + name + ", " + city + ")"
}

// Define an implicit ordering on Entry
implicit val entryOrdering = new Ordering[Entry] {
  def compare(x: Entry, y: Entry): Int = {
    val c = x.name.compareTo(y.name)
    if (c != 0)
      c
    else
      x.city.compareTo(y.city)
  }
}

val t = new ExpDecayTrend[Entry](1000, 1000L)
t.addIndex("name", IndexMap((entry: Entry) => entry.name))
t.addIndex("city", IndexMap((entry: Entry) => entry.city))

t.update(new Entry("Steve", "Berlin"), 0L)
t.update(new Entry("Frank", "London"), 1000L)
t.update(new Entry("Steve", "Madrid"), 2000L)
t.update(new Entry("Paul", "Berlin"), 3000L)

println("All entries for people called Steve: " + t.queryIndexWithScore("name", "Steve", 10, 0))
println("All entries for people from Berlin: " + t.queryIndex("city", "Berlin", 10, 0))
