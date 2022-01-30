package com.databricks.spark.sql.perf.utils

import com.databricks.spark.sql.perf.BlockingLineStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File

object Utils {
  var TPC_DS_PATH = ""
  var FILE_NAME = "DSGen-software-code-3.2.0rc1-compiled.zip"
  var UNZIP_NAME = "DSGen-software-code-3.2.0rc1"
  var TPC_DS_INSTALL_PATH = "/tmp/tpc_ds"
  val configuration = getHadoopConf()

  def setTpcdsPath(hdfsPath: String) = TPC_DS_PATH = hdfsPath
  def setFileName(fileName: String) = FILE_NAME = fileName
  def setUnzipFileName(fileName: String) = UNZIP_NAME = fileName
  def setInstallPath(path: String) = TPC_DS_INSTALL_PATH = path

  def getHadoopConf(): Configuration = {
    val dir = sys.props.get("HADOOP_CONF_DIR")
    val conf = new Configuration()
    conf.addResource(dir + "hdfs-site.xml")
    conf.addResource(dir + "core-site.xml")
    conf
  }

  def installTpcds(): String = {
    // 使用默认路径
    val fs = new Path(TPC_DS_PATH).getFileSystem(configuration)
    new File(TPC_DS_INSTALL_PATH).mkdir()
    // 下载
    fs.copyToLocalFile(new Path(TPC_DS_PATH),
      new Path(TPC_DS_INSTALL_PATH + "/" + FILE_NAME))

    val commands = Seq(
      "bash", "-c",
      s"cd $TPC_DS_INSTALL_PATH && unzip $FILE_NAME")
    println(commands)
    BlockingLineStream(commands)
    TPC_DS_INSTALL_PATH + "/" + UNZIP_NAME + "/tools"
  }

  def copyFromLocalDir(localDir: String, hdfsDir: String): Unit = {
    val sourcePath = new Path(localDir)
    val targetPath = new Path(hdfsDir)
    val fs = targetPath.getFileSystem(configuration)
    if (!fs.exists(targetPath)) {
      fs.mkdirs(targetPath)
    } else if (fs.isFile(targetPath)) {
      return
    }

    val localFs = FileSystem.getLocal(new Configuration())
    if (!localFs.exists(sourcePath)) {
      return
    }
    localFs.listStatus(sourcePath).foreach(f => {
      fs.copyFromLocalFile(f.getPath, new Path(targetPath, f.getPath.getName))
    })
  }

  def listLocalFiles(localDir: String): Seq[String] = {
    val localFs = FileSystem.getLocal(new Configuration())
    val localPath = new Path(localDir)
    if (!localFs.exists(localPath)) {
      return new Array[String](0)
    }
    localFs.listStatus(localPath)
      .map(f => {
        f.getPath.getName
      })
  }

}
