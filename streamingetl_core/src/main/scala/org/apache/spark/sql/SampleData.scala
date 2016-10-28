package org.apache.spark.sql

/**
  * Created by hzyuemeng1 on 2016/10/24.
  */
object SampleData {
  def apply(df:DataFrame):String = {
    df.showString(5,true)
  }

}
