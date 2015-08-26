package org.apache.spark.storage

import org.apache.spark.Logging
import org.apache.spark.scheduler.TaskSetManager
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RemoveAndAddResultCommand, RemoveFetchCommand}
import org.apache.spark.storage.ShuffleBlockFetcherIterator.{FetchRequest, SuccessFetchResult}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Created by Feiyu on 2015/8/18. **/
private[spark] class SkewTuneWorker(val executorID: String,
                                    private val backend: SkewTuneBackend,
                                    val fetchIterator: ShuffleBlockFetcherIterator,
                                    val taskId: Long) extends Logging {

  val blocks = new mutable.HashMap[BlockId, SkewTuneBlockInfo]()

  def reportBlockStatuses(seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportBlockStatuses seq $seq")
    backend.reportBlockStatuses(taskId, seq, newTaskId)
  }

  def reportTaskFinished(): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportTaskFinished")
    backend.reportTaskFinished(taskId)
  }

  def registerNewTask(seq: Seq[SkewTuneBlockInfo]): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : registerNewTask seq $seq")
    backend.registerNewTask(taskId, executorID, seq)
  }
}

private[spark] trait SkewTuneBackend {
  def reportBlockStatuses(taskID: Long, seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None)

  def reportTaskFinished(taskID: Long)

  def registerNewTask(taskID: Long, executorId: String, seq: Seq[SkewTuneBlockInfo])
}

private[spark] class SkewTuneMaster(val taskSetManager: TaskSetManager) extends Logging {

  val activeTasks = new mutable.HashMap[Long, SkewTuneTaskInfo] //sbt:error private class escapes its defining scope。因为SkewTuneTaskInfo是private，但是activeTask是public，所以内部类逃逸了

  def computerAndSplit(isLastTask: Boolean): (Seq[RemoveFetchCommand], Seq[RemoveAndAddResultCommand]) = {
    logInfo(s"Master ${taskSetManager.name} computerAndSplit")

    val transferFetches = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResults = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]

    val sortedTasks = new ListBuffer[SkewTuneTaskInfo] ++ activeTasks.values.toList.sortBy(_.blockMap.map(_._2.blockSize).sum).reverse
    val taskToSplit = if (sortedTasks.length >= 2) Some(sortedTasks.remove(0)) else None
    val secondTask = sortedTasks.head
    val maxSizeLine = secondTask.blockMap.map(_._2.blockSize).sum
    val blocksToSchedule = taskToSplit.get.blockMap.filter(blockInfo =>
      blockInfo._2.blockState == SkewTuneBlockStatus.LOCAL_FETCHED
        || blockInfo._2.blockState == SkewTuneBlockStatus.REMOTE_FETCHED
        || blockInfo._2.blockState == SkewTuneBlockStatus.REMOTE_FETCH_WAITING)
    val blockSizeNotSchedulable = taskToSplit.get.blockMap.map(_._2.blockSize).sum - blocksToSchedule.map(_._2.blockSize).sum

    val fetchCommands = new ArrayBuffer[RemoveFetchCommand]()
    val resultCommands = new ArrayBuffer[RemoveAndAddResultCommand]()

    minimumCostSchedule()
    if (isLastTask)
      maximumFairnessSchedule() //sbt :Error :forward reference extends over definition of value fetchCommands。在fetchCommands定义之前就引用了它

    //8.26 现在按照大小，不考虑cost，粗粒度的切割，速度快
    def minimumCostSchedule(): Unit = {
      val tasksToAddBlock = sortedTasks.drop(1)
      for (task <- tasksToAddBlock if blockSizeNotSchedulable + blocksToSchedule.map(_._2.blockSize).sum > maxSizeLine) {
        val sizeNeed = maxSizeLine - task.blockMap.map(_._2.blockSize).sum
        var sizeAdded: Long = 0
        for (blockInfo <- blocksToSchedule if sizeAdded <= sizeNeed) {
          sizeAdded = sizeAdded + blockInfo._2.blockSize
          blockInfo._2.blockState match {
            case SkewTuneBlockStatus.REMOTE_FETCH_WAITING =>
              val blocks = transferFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]())
              blocks += blockInfo._2
            case _ =>
              if (taskToSplit.get.executorId == task.executorId) {
                val blocks = transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]())
                blocks += blockInfo._2
              }
          }
          blocksToSchedule -= blockInfo._1
        }
      }
      logInfo(s"minimumCostSchedule : Fetches : $transferFetches , Results : $transferResults")
    }
    //除了第一个task，其他task的size一致，然后再次分配使所有task的block的总size相等
    def maximumFairnessSchedule(): Unit = {
      val sizeToAddAvg = (blocksToSchedule.map(_._2.blockSize).sum - maxSizeLine) / (sortedTasks.length + 1)
      //8.26 如果所有task都分了还有剩余的，平均分配给所有task
      for (task <- sortedTasks if blocksToSchedule.map(_._2.blockSize).sum > sizeToAddAvg) {
        var sizeAdded: Long = 0
        for (blockInfo <- blocksToSchedule if sizeAdded <= sizeToAddAvg) {
          sizeAdded = sizeAdded + blockInfo._2.blockSize
          blockInfo._2.blockState match {
            case SkewTuneBlockStatus.REMOTE_FETCH_WAITING =>
              val blocks = transferFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]())
              blocks += blockInfo._2
            case _ =>
              if (taskToSplit.get.executorId == task.executorId) {
                val blocks = transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]())
                blocks += blockInfo._2
              }
          }
          blocksToSchedule -= blockInfo._1
        }
      }
      logInfo(s"maximumFairnessSchedule : Fetches : $transferFetches , Results : $transferResults")
    }
    //8.26 转换成命令数组
    transferFetches.foreach(info => {
      val blocksByAddress = new mutable.HashMap[BlockManagerId, ArrayBuffer[BlockId]]
      info._2.foreach(block => {
        blocksByAddress.getOrElseUpdate(block.blockManagerId, new ArrayBuffer[BlockId]()) += block.blockId
      })
      fetchCommands += RemoveFetchCommand(info._1._2.executorId,
        info._1._2.taskId,
        info._1._1.taskId,
        blocksByAddress.toSeq)
    })
    transferResults.foreach(info => {
      resultCommands += RemoveAndAddResultCommand(info._2.map(_.blockId), info._1._1.taskId, info._1._2.taskId)
    })
    logInfo(s"computerAndSplit : fetchCommands $fetchCommands , resultCommands : $resultCommands")
    (fetchCommands, resultCommands)

  }

  def registerNewTask(taskID: Long, executorId: String, seq: Seq[SkewTuneBlockInfo]): Unit = {
    logInfo(s"registerNewTask : task $taskID , executorId $executorId , seq $seq")
    if (!activeTasks.contains(taskID)) {
      val blockMap: mutable.Map[BlockId, SkewTuneBlockInfo] = new mutable.HashMap[BlockId, SkewTuneBlockInfo]()
      blockMap ++= seq.map(info => (info.blockId, info))
      activeTasks += ((taskID, SkewTuneTaskInfo(taskID, executorId, blockMap)))
    }
  }

  def reportBlockStatuses(taskID: Long, seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None): Unit = {
    logInfo(s"reportBlockStatuses : task $taskID , seq $seq , newTaskID : $newTaskId")
    if (activeTasks.contains(taskID)) {
      val blockMap = activeTasks.get(taskID).get.blockMap
      val blockMapInNewTask: Option[mutable.Map[BlockId, SkewTuneBlockInfo]] = if (newTaskId.nonEmpty && activeTasks.contains(newTaskId.get)) Option(activeTasks.get(newTaskId.get).get.blockMap) else None

      for ((blockId, newBlockState) <- seq if blockMap.contains(blockId)) {
        val skewTuneBlockInfo = blockMap.get(blockId).get
        skewTuneBlockInfo.blockState = if (newBlockState > 0) newBlockState else skewTuneBlockInfo.blockState
        if (blockMapInNewTask.nonEmpty) {
          blockMapInNewTask.get += ((blockId, skewTuneBlockInfo))
          blockMap -= blockId
        }
      }
    }
  }

  def reportTaskFinished(taskID: Long): Unit = {
    logInfo(s"reportTaskFinished : task $taskID")
    if (activeTasks.contains(taskID)) {
      activeTasks -= taskID
    }
  }

  class SkewTuneTaskInfo(val taskId: Long, val executorId: String, val blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]) {}

  object SkewTuneTaskInfo {
    def apply(taskId: Long, executorId: String, blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]): SkewTuneTaskInfo = {
      new SkewTuneTaskInfo(taskId, executorId, blockMap)
    }
  }

}

private[spark] object SkewTuneBlockStatus {
  val LOCAL_FETCHED: Byte = 0x01
  val REMOTE_FETCH_WAITING: Byte = 0x02
  val REMOTE_FETCHING: Byte = 0x03
  val REMOTE_FETCHED: Byte = 0x04
  val USED: Byte = 0x05
}

private[spark] class SkewTuneBlockInfo(val blockId: BlockId,
                                       val blockSize: Long,
                                       val blockManagerId: BlockManagerId,
                                       var blockState: Byte,
                                       var inWhichFetch: Option[FetchRequest],
                                       var inWhichResult: Option[SuccessFetchResult]) {
  def name = "SkewTuneBlockInfo_" + blockId + "_on_" + blockManagerId
}

private[spark] object SkewTuneBlockInfo {
  def apply(blockId: BlockId, blockSize: Long, blockManagerId: BlockManagerId, blockState: Byte,
            inWhichFetch: Option[FetchRequest], inWhichResult: Option[SuccessFetchResult]): SkewTuneBlockInfo = {
    new SkewTuneBlockInfo(blockId, blockSize, blockManagerId, blockState, inWhichFetch, inWhichResult)
  }
}

/*
private[spark] class Long(val stageId: Int, val partitionId: Int) {
  def name = "SkewTuneTaskId_" + stageId + "_" + partitionId
}

private[spark] object SkewTuneTaskId {
  def apply(stageId: Int, partitionId: Int): SkewTuneTaskId = {
    new SkewTuneTaskId(stageId, partitionId)
  }
}*/
