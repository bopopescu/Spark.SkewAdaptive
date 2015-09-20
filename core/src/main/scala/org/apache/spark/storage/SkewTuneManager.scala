package org.apache.spark.storage

import org.apache.spark.Logging
import org.apache.spark.scheduler.TaskSetManager
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RemoveAndAddResultCommand, RemoveFetchCommand}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Created by Feiyu on 2015/8/18. **/
private[spark] class SkewTuneWorker(val executorID: String,
                                    private val backend: SkewTuneBackend,
                                    var fetchIterator: ShuffleBlockFetcherIterator,
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

  def reportTaskComputeSpeed(speed: Float): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportTaskComputeSpeed $speed Byte/ms")
    backend.reportTaskComputeSpeed(taskId, executorID, speed)
  }

  def reportBlockDownloadSpeed(fromExecutor: String, speed: Float): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportBlockDownloadSpeed $speed Byte/ms")
    backend.reportBlockDownloadSpeed(fromExecutor, executorID, speed)
  }
}

private[spark] trait SkewTuneBackend {
  def reportBlockStatuses(taskID: Long, seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None)

  def reportTaskFinished(taskID: Long)

  def registerNewTask(taskID: Long, executorId: String, seq: Seq[SkewTuneBlockInfo])

  //9.3 SkewTuneAdd
  def reportTaskComputeSpeed(taskId: Long, executorId: String, speed: Float)

  def reportBlockDownloadSpeed(fromExecutor: String, toExecutor: String, speed: Float)
}

private[spark] class SkewTuneMaster(val taskSetManager: TaskSetManager,
                                    val networkSpeed: mutable.HashMap[(String, String), Float]) extends Logging {

  //activeTask接受注册时blockMap为空的task，因为未来可能给它分配skewtuneTask
  val activeTasks = new mutable.HashMap[Long, SkewTuneTaskInfo] //sbt:error private class escapes its defining scope。因为SkewTuneTaskInfo是private，但是activeTask是public，所以内部类逃逸了
  //9.19 SkewTuneAdd
  val isRegistered = new mutable.HashMap[Long,Boolean]()
  val demonTasks = new mutable.HashSet[Long]()
  var taskFinishedOrRunning = 0

  //9.5 SkewTuneAdd 在taskset级别上记录某个executor上面的compute速度
  val computeSpeed = new mutable.HashMap[String, Float]()
  private val TIME_SCHEDULE_COST_OF_FETCH = 0
  private val TIME_SCHEDULE_COST_OF_RESULT = 0

  private def costOfSchedule(blockInfo: SkewTuneBlockInfo, oldExecutor: String, newExecutor: String): Int = {
    val blockAddress = blockInfo.blockManagerId.executorId
    //9.6 如果是fetched，只能选择transferResult
    if ((blockInfo.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0) {
      val costDiff = TIME_SCHEDULE_COST_OF_RESULT
      costDiff.toInt
      //9.6 如果是wait——fetch，而且都在同一个executor上
    } else if (oldExecutor == newExecutor) {
      val costDiff = TIME_SCHEDULE_COST_OF_FETCH
      costDiff.toInt
      //9.6 如果是wait_fetch，而且不在同一个executor上
    } else {
      val costDiff = TIME_SCHEDULE_COST_OF_FETCH + blockInfo.blockSize / networkSpeed.getOrElse((blockAddress, newExecutor), Float.MaxValue)
      -blockInfo.blockSize / networkSpeed.getOrElse((blockAddress, oldExecutor), Float.MaxValue)
      +blockInfo.blockSize / computeSpeed.getOrElse(newExecutor, Float.MaxValue)
      -blockInfo.blockSize / computeSpeed.getOrElse(oldExecutor, Float.MaxValue)
      costDiff.toInt
    }
  }

  //9.5 对一个block Seq统计所需完成时间，不考虑已经used的block 和 maxFlightInKb对Fetching的限制
  //9.14 LOCAL_FETCHED/REMOTE_FETCHED 考虑计算速度。REMOTE_FETCH_WAITING/REMOTE_FETCHING 考虑计算速度和下载速度。其他0
  private def timeBySeq(blockInfo: Seq[SkewTuneBlockInfo], onWhichExecutor: String): Int = {
    blockInfo.map(info =>
      if ((info.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0)
        (info.blockSize / computeSpeed.getOrElse(onWhichExecutor, Float.MaxValue)).toInt
      else if ((info.blockState & (SkewTuneBlockStatus.REMOTE_FETCH_WAITING | SkewTuneBlockStatus.REMOTE_FETCHING)) != 0)
        (info.blockSize / networkSpeed.getOrElse((info.blockManagerId.executorId, onWhichExecutor), Float.MaxValue) +
          info.blockSize / computeSpeed.getOrElse(onWhichExecutor, Float.MaxValue)).toInt
      else
        0
    ).sum
  }

  //9.5 加入了计算速度和下载速度作为调度block的cost基础
  def computerAndSplit(isLastTask: Boolean): Option[(Seq[RemoveFetchCommand], Seq[RemoveAndAddResultCommand], Long, Long)] = {
    logInfo(s"Master on taskSet ${taskSetManager.name} computerAndSplit with COST. activeTaskNumber: ${activeTasks.keySet}")

    val start_time = System.currentTimeMillis()
    val transferFetches = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResults = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]

    //9.5 按照完成任务所需时间从大到小排序
    val sortedTasks = new ArrayBuffer[SkewTuneTaskInfo] ++= activeTasks.values.toList
      .sortBy(task => timeBySeq(task.blockMap.values.toSeq, task.executorId)).reverse
    val taskToSplit = if (sortedTasks.length >= 2) Some(sortedTasks.remove(0)) else None

    //8.31 taskToSpilt必须有block时才可以分割
    if (taskToSplit.isDefined && taskToSplit.get.blockMap.nonEmpty) {

      //9.5 这三种status的block可以被调度，任何时候used的block不在考虑范围内
      val blocksToSchedule = taskToSplit.get.blockMap.filter(blockInfo =>
        (blockInfo._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED
          | SkewTuneBlockStatus.REMOTE_FETCH_WAITING)) != 0)
      /*val blockTimeCostNotSchedulable = timeBySeq(taskToSplit.get.blockMap.values
        .filter(_.blockState == SkewTuneBlockStatus.REMOTE_FETCHING).toSeq, taskToSplit.get.executorId)*/
      //if (isLastTask)
      //maximumFairnessSchedule() //sbt :Error :forward reference extends over definition of value fetchCommands。在fetchCommands定义之前就引用了它
      val fetchCommands = new ArrayBuffer[RemoveFetchCommand]()
      val resultCommands = new ArrayBuffer[RemoveAndAddResultCommand]()

      if (sortedTasks.length >= 1) {
        val secondTask = sortedTasks.head
        var firstTaskTime = timeBySeq(taskToSplit.get.blockMap.values.toSeq, taskToSplit.get.executorId)
        var maxTimeCostLine = timeBySeq(secondTask.blockMap.values.toSeq, secondTask.executorId)

        minimumCostSchedule()

        //9.6 对每个block,找到调度开销最小的目标task列表，检查task是否还能添加新的block进去直到找到一个可行task
        def minimumCostSchedule(): Unit = {
          val tasksToAddBlock = sortedTasks.reverse
          val taskTime = new mutable.HashMap[Long, Long]()
          for (block <- blocksToSchedule if firstTaskTime > maxTimeCostLine) {
            val tasks = tasksToAddBlock.sortBy(info => costOfSchedule(block._2, block._2.blockManagerId.executorId, info.executorId))
            val taskOp = tasks.find(task => {
              val time = timeBySeq(task.blockMap.values.toSeq, task.executorId)
              taskTime += ((task.taskId, time))
              if (maxTimeCostLine - time > 0) true else false
            })
            if (taskOp.isDefined) {
              val task = taskOp.get
              if ((block._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0) {
                if (block._2.blockManagerId.executorId == task.executorId) {
                  transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                  firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                  val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                  taskTime.update(task.taskId, newTime)
                  maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
                }
              } else {
                transferFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                taskTime.update(task.taskId, newTime)
                maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
              }
            }
            blocksToSchedule -= block._1
          }
          logInfo(s"\t\tminimumCostSchedule : Fetches : ${transferFetches.size} , Results : ${transferResults.size}")
        }
      }
      /*else {
        balanceScheduleFor2Tasks()

        //9.14 只有2个active task时进行公平调度
        def balanceScheduleFor2Tasks(): Unit = {
          var timeOfTaskToSchedule = timeBySeq(blocksToSchedule.values.toSeq, taskToSplit.get.executorId)
          val task = sortedTasks.last
          var timeOfOtherTask = timeBySeq(task.blockMap.values.toSeq, task.executorId)
          //9.6 如果最后还多余block待分配且是最后一个task，按照随机分配方法分配给各个task，以期公平
          for (block <- blocksToSchedule if timeOfTaskToSchedule > timeOfOtherTask) {
            if ((block._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0)
              transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
            else
              transferFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2

            timeOfTaskToSchedule -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
            timeOfOtherTask += timeBySeq(Seq(block._2), task.executorId)
          }
          logInfo(s"balanceScheduleFor2Tasks : Fetches : $transferFetches , Results : $transferResults")
        }
      }*/
      //8.26 转换成命令数组
      transferFetches.foreach(info => {
        val blocksByAddress = new mutable.HashMap[BlockManagerId, ArrayBuffer[BlockId]]
        info._2.foreach(block => {
          blocksByAddress.getOrElseUpdate(block.blockManagerId, new ArrayBuffer[BlockId]()) += block.blockId
        })
        fetchCommands += RemoveFetchCommand(info._1._2.executorId, info._1._2.taskId, info._1._1.taskId, blocksByAddress.toSeq)
      })

      transferResults.foreach(info => {
        resultCommands += RemoveAndAddResultCommand(info._2.map(_.blockId), info._1._1.taskId, info._1._2.taskId)
      })

      val time_used = System.currentTimeMillis() - start_time

      if (fetchCommands.nonEmpty || resultCommands.nonEmpty) {
        logInfo(s"\t\tcomputerAndSplit : fetchCommands $fetchCommands , resultCommands : $resultCommands . time : $time_used ms")
        Some((fetchCommands, resultCommands,taskToSplit.get.taskId,sortedTasks.head.taskId))
      } else {
        logInfo(s"\t\tcomputerAndSplit Terminate: fetchCommands / resultCommands all empty . time : $time_used ms")
        None
      }
    } else {
      logInfo(s"\t\tcomputerAndSplit Terminate: taskToSplit is empty or its blockMap is empty.")
      None
    }
  }

  @Deprecated
  def computerAndSplitOnlyUseBlockSize(isLastTask: Boolean): Option[(Seq[RemoveFetchCommand], Seq[RemoveAndAddResultCommand])] = {
    logInfo(s"Master on taskSet ${
      taskSetManager.name
    } computerAndSplit")
    val transferFetches = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResults = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]

    val sortedTasks = new ListBuffer[SkewTuneTaskInfo] ++= activeTasks.values.toList
      .sortBy(_.blockMap.map(_._2.blockSize).sum).reverse
    val taskToSplit = if (sortedTasks.length >= 3) Some(sortedTasks.remove(0)) else None

    //8.31 taskToSpilt必须有block时才可以分割
    if (taskToSplit.isDefined && taskToSplit.get.blockMap.nonEmpty) {
      val secondTask = sortedTasks.head
      //9.5 SkewTuneAdd : bug 应该把used的block排除在外
      val maxSizeLine = secondTask.blockMap.map(info => if (info._2.blockState != SkewTuneBlockStatus.USED) info._2.blockSize else 0).sum
      val blocksToSchedule = taskToSplit.get.blockMap.filter(blockInfo =>
        (blockInfo._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED
          | SkewTuneBlockStatus.REMOTE_FETCH_WAITING)) != 0)
      val blockSizeNotSchedulable = taskToSplit.get.blockMap.map(info => if (info._2.blockState != SkewTuneBlockStatus.USED) info._2.blockSize else 0).sum - blocksToSchedule.map(_._2.blockSize).sum

      val fetchCommands = new ArrayBuffer[RemoveFetchCommand]()
      val resultCommands = new ArrayBuffer[RemoveAndAddResultCommand]()

      minimumCostScheduleWithSize()
      if (isLastTask)
        maximumFairnessScheduleWithSize() //sbt :Error :forward reference extends over definition of value fetchCommands。在fetchCommands定义之前就引用了它

      //8.26 现在按照大小，不考虑cost，粗粒度的切割，速度快
      def minimumCostScheduleWithSize(): Unit = {
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
        logInfo(s"minimumCostScheduleWithSize : Fetches : $transferFetches , Results : $transferResults")
      }
      //除了第一个task，其他task的size一致，然后再次分配使所有task的block的总size相等
      def maximumFairnessScheduleWithSize(): Unit = {
        val blocksSizeToSchedule = blocksToSchedule.map(_._2.blockSize).sum
        //8.27 条件：task1的size大于task2的size，task1的可分配size大于（task1-task2的size之差再平均的值）
        if (blocksSizeToSchedule + blocksSizeToSchedule > maxSizeLine) {
          val sizeToAddAvg = (blocksSizeToSchedule + blocksSizeToSchedule - maxSizeLine) / (sortedTasks.length + 1)
          if (sizeToAddAvg > 0 && blocksSizeToSchedule > sizeToAddAvg) {
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
          }
        }
        logInfo(s"maximumFairnessScheduleWithSize : Fetches : $transferFetches , Results : $transferResults")
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

      if (fetchCommands.nonEmpty || resultCommands.nonEmpty) {
        logInfo(s"computerAndSplit : fetchCommands $fetchCommands , resultCommands : $resultCommands")
        Some((fetchCommands, resultCommands))
      } else {
        logInfo(s"computerAndSplit Terminate: fetchCommands / resultCommands all empty")
        None
      }
    } else {
      None
    }
  }

  def registerNewTask(taskID: Long, executorId: String, seq: Seq[SkewTuneBlockInfo]): Unit = {
    logInfo(s"[########] on taskSet ${taskSetManager.name}:registerNewTask : task $taskID, executorId $executorId, seq ${seq.size}")
      if (!activeTasks.contains(taskID)) {
        synchronized {
          val blockMap = new mutable.HashMap[BlockId, SkewTuneBlockInfo]() ++= seq.map(info => (info.blockId, info))
          activeTasks += ((taskID, SkewTuneTaskInfo(taskID, executorId, blockMap)))
          isRegistered += ((taskID, true))
          taskFinishedOrRunning += 1
      }
    }

  }

  def reportBlockStatuses(taskId: Long, seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None): Unit = {
    if (newTaskId.isDefined)
      logInfo(s"on taskSet ${taskSetManager.name}:reportBlockStatuses : task $taskId, seq $seq, newTaskID: $newTaskId")
    if (activeTasks.contains(taskId)) {
      val blockMapInOldTask = activeTasks.get(taskId).get.blockMap
      val blockMapInNewTask: Option[mutable.Map[BlockId, SkewTuneBlockInfo]] =
        if (newTaskId.nonEmpty && activeTasks.contains(newTaskId.get)) Some(activeTasks(newTaskId.get).blockMap) else None

      for ((blockId, newBlockState) <- seq if blockMapInOldTask.contains(blockId)) {
        val skewTuneBlockInfo = blockMapInOldTask(blockId)
        skewTuneBlockInfo.blockState = if (newBlockState > 0) newBlockState else skewTuneBlockInfo.blockState
        if (blockMapInNewTask.nonEmpty && taskId != newTaskId.get) {
          blockMapInNewTask.get += ((blockId, skewTuneBlockInfo))
          blockMapInOldTask -= blockId
        }
      }
    }
  }

  def reportTaskFinished(taskID: Long): Unit = {
    logInfo(s"on taskSet ${taskSetManager.name}: reportTaskFinished: task $taskID")
    if (activeTasks.contains(taskID)) {
      activeTasks -= taskID
    }
  }

  def reportTaskComputerSpeed(taskId: Long, executorId: String, speed: Float): Unit = {
    logInfo(s"on taskSet ${taskSetManager.name}: reportTaskComputeSpeed for task $taskId on Executor $executorId with speed $speed byte/ms")
    val lastSpeed = computeSpeed.getOrElseUpdate(executorId, speed)
    computeSpeed += ((executorId, (lastSpeed + speed) / 2))
  }

  class SkewTuneTaskInfo(val taskId: Long, val executorId: String, val blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]) extends Serializable {
    override def toString = s"SkewTuneTask_$taskId :mapSize_${blockMap.size}_onExecutor_$executorId"
  }

  object SkewTuneTaskInfo {
    def apply(taskId: Long, executorId: String, blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]): SkewTuneTaskInfo = {
      new SkewTuneTaskInfo(taskId, executorId, blockMap)
    }
  }

}

private[spark] object SkewTuneBlockStatus {
  val LOCAL_FETCHED: Byte = 1
  val REMOTE_FETCH_WAITING: Byte = 2
  val REMOTE_FETCHING: Byte = 4
  val REMOTE_FETCHED: Byte = 8
  val USED: Byte = 16
}

private[spark] class SkewTuneBlockInfo(val blockId: BlockId,
                                       val blockSize: Long,
                                       val blockManagerId: BlockManagerId,
                                       var blockState: Byte /*,
                                       var inWhichFetch: Option[FetchRequest],
                                       var inWhichResult: Option[SuccessFetchResult]*/) extends Serializable {
  override def toString = s"SkewTuneBlockInfo_$blockId :blockSize_${blockSize}_onExecutor_${blockManagerId.executorId}"
}

private[spark] object SkewTuneBlockInfo {
  def apply(blockId: BlockId, blockSize: Long, blockManagerId: BlockManagerId, blockState: Byte /*,
            inWhichFetch: Option[FetchRequest], inWhichResult: Option[SuccessFetchResult]*/): SkewTuneBlockInfo = {
    new SkewTuneBlockInfo(blockId, blockSize, blockManagerId, blockState /*, inWhichFetch, inWhichResult*/)
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
