package parallel
import common._

/**
  * Created by inakov on 28.10.16.
  */
object MargeSort extends App{

  def quickSort(array: Array[Int], from: Int, until: Int): Unit = {
    def partition(start: Int, end: Int): Int ={
      def swap(a: Int, b: Int): Unit = {
        val tmp = array(b)
        array(b) = array(a)
        array(a) = tmp
      }

      val pivot = array(end)
      var firstHigher = start
      var index = start
      while(index < end){
        if(array(index) <= pivot){
          swap(index, firstHigher)
          firstHigher += 1
        }
        index += 1
      }
      swap(firstHigher, end)
      firstHigher
    }

    if(until - from >= 2){
      val pivot = partition(from, until - 1)
      quickSort(array, from, pivot)
      quickSort(array, pivot + 1, until)
    }

  }

  def merge(src: Array[Int], dst: Array[Int], from: Int, mid: Int, until: Int): Unit = {
    var currentLeft = from
    val leftEnd = mid
    var currentRight = mid
    val rightEnd = until

    var index = from
    while(index < until){
      val nextElem = if(currentLeft >= leftEnd){
        val elem = src(currentRight)
        currentRight += 1
        elem
      }else if(currentRight >= rightEnd){
        val elem = src(currentLeft)
        currentLeft += 1
        elem
      }else if(src(currentLeft) < src(currentRight)){
        val elem = src(currentLeft)
        currentLeft += 1
        elem
      }else{
        val elem = src(currentRight)
        currentRight += 1
        elem
      }
      dst(index) = nextElem
      index += 1
    }
  }

  def parallelMargeSort(xs: Array[Int]): Unit = {
    val maxDepth = 4
    val ys = new Array[Int](xs.length)

    def sort(from: Int, until: Int, depth: Int): Unit = {
      if(depth == maxDepth){
        quickSort(xs, from, until)
      }else{
        val mid = (from + until) / 2
        parallel(sort(mid, until, depth + 1), sort(from, mid, depth + 1))
        val flip = (maxDepth - depth) % 2 == 0
        val src = if(flip) ys else xs
        val dest = if(flip) xs else ys
        merge(src, dest, from, mid, until)
      }
    }

    sort(0, xs.length, 0)
  }

  val xs = Array[Int](9, 7, 5, 11, 12, 2, 14, 3, 10, 6)
  println("Input: " + xs.mkString(", "))
  parallelMargeSort(xs)
  println("Output: " + xs.mkString(", "))
}
