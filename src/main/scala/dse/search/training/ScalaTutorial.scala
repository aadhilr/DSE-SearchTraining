abstract class Item {

  def value() = {
    print("value")
  }
}

case class Article (name: String, price: Double) extends Item

case class Multiple(num: Int, item: Item) extends Item

case class Bundles(num: Int, item: Item)  extends Item

def price(item: Item) = item match {
  case m: Multiple => m.value()
  case b: Bundles => b.value()
}

val item = new Item{}
item.value()

object Test {

  def main(args: Array[String]): Unit = {
    swapWithGrouped(Array(1,2,3,4,5))
    withFOrYield()
    removeDuplicates(Array(2,5,5,8,1,3,9,1,9,5))
    minMax(Array(2,5,5,8,1,3,9,1,9,5))
    returnNumber(Array("Tom","Fred", "Harry"), Map("Tom" -> 3, "Dick" -> 4, "Harry" -> 5))
  }



  def signum(x: Int): Int = {
    if(x > 1){
      1
    } else if (x < 1) {
      -1
    } else {
      0
    }
  }

  def swapWithGrouped(a: Array[Int]): Unit = {
    val swapped = a.grouped(2).flatMap {
      case Array(x, y) => Array(y, x)
      case single => single
    }.toArray
    print(swapped.deep.mkString("\n"))

  }

  def withFOrYield() = {
    val swapped = (for {
      i <- Array(1,2,3,4,5).sliding(2,2)
      j <- i.reverse
    } yield j).toArray

    print(swapped.deep.mkString("\n"))
  }

  def removeDuplicates(a:Array[Int]) = {
    print(a.distinct.deep)
  }

  def minMax(a:Array[Int]) = {
    print(a.min+","+a.max)
  }

  def returnNumber(a: Array[String], b: Map[String, Int]) = {
    print(a.flatMap(x=>b.get(x)).deep)

  }


  def mkString[T](iterable: collection.Iterable[T], split: String = ",") = {
    print(iterable.map(x=>x.toString).reduceLeft(_ + split + _))
  }
}

class Person(firstName: String, lastName: String) {

  object Person {
    def apply(firstLast: String) = {
      val firstLastArray = firstLast.trim.split(" ")
      new Person(firstLastArray(0), firstLastArray(1))
    }
  }
}

class Employee1(val name: String, var salary: Double) {
  def this() {
    this("John Q. Public", 0.0)
  }
}

class Employee(name: String = "John Q. Public", sal: Double = 0.0){
  val empName = name;
  val empSal = sal
}





