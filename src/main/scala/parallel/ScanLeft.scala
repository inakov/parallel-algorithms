package parallel

import common._

/**
  * Created by inakov on 31.10.16.
  */
object ScanLeft extends App{

  def scanLeftSeq[A](input: Array[A], output: Array[A], a0: A, f:(A, A) => A): Unit = {
    output(0) = a0

    var index = 0
    while(index < input.length){
      val nextElem = f(output(index), input(index))
      index += 1
      output(index) = nextElem
    }
  }

  //Implementation of scanLeft by map and reduce function
  //This approach doubles the work, but can be efficient
  // when map and reduce are implemented in parallel
  def scanLeftMapAndReduce[A](input: Array[A], output: Array[A], a0: A, f:(A, A) => A): Unit = {

    def reduceSeg(from: Int, until: Int, a0: A, f:(A, A) => A): A = {
      var acc = a0
      var index = from
      while(index < until){
        acc = f(acc, input(index))
        index += 1
      }

      acc
    }

    def mapSeg(from: Int, until: Int, f:(Int, A) => A): Unit = {
      var index = from
      while(index < until){
        output(index) = f(index, input(index))

        index+=1
      }
    }

    def fi(index: Int, elem: A): A = {
      reduceSeg(0, index, a0, f)
    }

    mapSeg(0, input.length, fi)
    val last = input.length - 1
    output(last + 1) = f(output(last), input(last))
  }
  //scanLeft implementation based on reduction tree
  def scanLeft[A](input: Tree[A], a0: A, f:(A, A) => A): Tree[A] = {
    def upsweep(t: Tree[A], f:(A, A) => A): ResultTree[A] = t match {
      case Leaf(value) => ResultLeaf(value)
      case Node(l, r) =>
        val (resultLeft, resultRight) = parallel(upsweep(l, f), upsweep(r, f))
        ResultNode(f(resultLeft.res, resultRight.res), resultLeft, resultRight)
    }

    def downsweep(t: ResultTree[A], a0: A, f:(A, A) => A): Tree[A] = t match {
      case ResultLeaf(v) => Leaf(f(a0, v))
      case ResultNode(_, l, r) =>
        val(leftRes, rightRes) = parallel(downsweep(l, a0, f),
          downsweep(r,f(a0, l.res),f))
        Node(leftRes, rightRes)
    }

    def prepend(a: A, t: Tree[A]): Tree[A] = t match {
      case leaf @ Leaf(v) => Node(Leaf(a), leaf)
      case Node(l, r) => Node(prepend(a, l), r)
    }
    val results = upsweep(input, f)

    prepend(a0, downsweep(results, a0, f))
  }

  val inputArr = Array(1, 3, 8)
  val outputArr = Array(0, 0, 0, 0)
  scanLeftMapAndReduce(inputArr, outputArr, 100, (x: Int, y: Int) => x + y)
  println(outputArr.mkString(", "))

  val inputTree = Node(Leaf(1), Node(Leaf(3), Leaf(8)))
  println(scanLeft(inputTree, 100,(x: Int, y: Int) => x + y))


}
