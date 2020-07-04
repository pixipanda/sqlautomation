package com.pixipanda.sqlautomation.handler.load

import com.pixipanda.sqlautomation.Spark
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.sqlautomation.handler.Handler


abstract class LoadHandler(query: String, saveConfig: SaveConfig) extends Handler with Spark{


}
