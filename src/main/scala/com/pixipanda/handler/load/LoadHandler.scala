package com.pixipanda.handler.load

import com.pixipanda.Spark
import com.pixipanda.sqlautomation.config.save.SaveConfig
import com.pixipanda.handler.Handler


abstract class LoadHandler(query: String, saveConfig: SaveConfig) extends Handler with Spark{


}
