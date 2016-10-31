package parallel
import common._

/**
  * Created by inakov on 28.10.16.
  */
object MapFunction extends App{

  def mapSegmentSeq[A, B](input: Array[A], output: Array[B], f: A => B, from: Int, until: Int): Unit ={
    var index = from
    while(index < until){
      output(index) = f(input(index))
      index += 1
    }
  }

  //Arrays version
  def mapSegmentParallel[A, B](input: Array[A], output: Array[B], f: A => B, from: Int, until: Int): Unit ={
    //Example value it isn't efficient to start parallel computation for such small segment!
    val threshold = 3

    if(until - from < threshold){
      mapSegmentSeq(input, output, f, from, until)
    }else{
      val mid = from + (until - from)/2
      parallel(mapSegmentParallel(input, output, f, from, mid), mapSegmentParallel(input, output, f, mid, until))
    }
  }

  def mapSegTree[A, B: Manifest](input: ArrayTree[A], f: A => B): ArrayTree[B] = {
    input match {
      case ArrayNode(left, right) =>
        val (l, r) = parallel(mapSegTree(left, f), mapSegTree(right, f))
        ArrayNode(l, r)
      case ArrayLeaf(content) =>
        val output = Array.ofDim[B](content.length)
        mapSegmentSeq(content, output, f, 0, content.length)
        ArrayLeaf(output)
    }
  }

  val in = Array(1,2,3,4,5,6,10)
  val out = Array(0,0,0,0,0,0,0)
  mapSegmentParallel(in, out, (x: Int) => x+x, 0, in.length)
  println(out.mkString(", "))

}
