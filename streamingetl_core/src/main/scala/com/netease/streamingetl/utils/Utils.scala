package com.netease.streamingetl.utils

/**
  * Created by hzyuemeng1 on 2016/10/18.
  */
private [streamingetl] object Utils {
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrETLClassLoader)
    // scalastyle:on classforname
  }


  def getContextOrETLClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
}
