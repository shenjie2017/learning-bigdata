package com.blue.make.utils


import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 13:53 2018/8/2
  * @Modifide By:
  **/

//      ┏┛ ┻━━━━━┛ ┻┓
//      ┃　　　　　　 ┃
//      ┃　　　━　　　┃
//      ┃　┳┛　  ┗┳　┃
//      ┃　　　　　　 ┃
//      ┃　　　┻　　　┃
//      ┃　　　　　　 ┃
//      ┗━┓　　　┏━━━┛
//        ┃　　　┃   神兽保佑
//        ┃　　　┃   代码无BUG！
//        ┃　　　┗━━━━━━━━━┓
//        ┃　　　　　　　    ┣┓
//        ┃　　　　         ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛

object MakeVehicleDataUtil {
  def main(args: Array[String]): Unit = {

    val filesLines:List[List[String]] = getFilesContent("D:\\tmp\\logs\\gps\\")

    //打算数据结果
    var maxRow = 0
    for(i <- 0 until filesLines.size){
      if(maxRow < filesLines.apply(i).size){
        maxRow = filesLines.apply(i).size
      }
    }

    var result:List[String] = Nil
    for(index<- 0 until maxRow){
      for(fileIndex <- 0 until filesLines.size){
        if(index<filesLines.apply(fileIndex).size) {
          result = result :+ filesLines.apply(fileIndex).apply(index)
        }
      }
    }

    val writer = new PrintWriter(new File("D:\\tmp\\logs\\gps\\gps.dat"))
    for(tmpString <- result){
      writer.println(tmpString)
    }

  }

  def getFilesContent(dir:String):List[List[String]]={
    var list:List[List[String]] = List()
    for(fileName <- subFiles(new File(dir))){
      val tmpList = getFileContent(fileName)
      list = list :+ tmpList
    }
    return list
  }

  def subFiles(dir:File):Array[String] ={
    return dir.listFiles().map(_.getPath)
  }

  def getFileContent(fileName:String):List[String]={
    var lines = List[String]()
    val file = Source.fromFile(fileName)

    for(line <- file.getLines()){
      lines = lines :+ line.toString
    }

    return lines
  }
}
