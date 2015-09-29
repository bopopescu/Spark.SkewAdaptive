package org.apache.spark.storage

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.executor.Executor
import org.apache.spark.scheduler.TaskSetManager
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RemoveResultAndAddResultCommand, RemoveResultAndAddFetchCommand, RemoveAndAddResultCommand, RemoveFetchCommand}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Created by Feiyu on 2015/8/18. **/
private[spark] class SkewTuneWorker(val executorID: String,
                                    private val backend: SkewTuneBackend,
                                    var fetchIterator: ShuffleBlockFetcherIterator,
                                    val taskId: Long) extends Logging {

  val blocks = new mutable.HashMap[BlockId, SkewTuneBlockInfo]()

  var executorInstance: Executor = _
  //9.26
  var fetchIndex = 0
  var allIteratorWorkTime = 0L

  def reportBlockStatuses(seq: Seq[(BlockId, Byte)], oldTaskId: Option[Long] = None, size: Option[Long] = None): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportBlockStatuses seq $seq")
    backend.reportBlockStatuses(taskId, seq, oldTaskId, size)
  }

  def reportTaskFinished(): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportTaskFinished")
    //allIteratorWorkTime += (if(fetchIterator == null) 0L else fetchIterator.totalTime)
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

  def reportNextFetchIterator(): Unit = {
    logInfo(s"SkewTuneWorker of task $taskId on executor $executorID : reportNextFetchIterator Index $fetchIndex")
    backend.reportNextFetchIterator(taskId, fetchIndex)
  }
}

private[spark] trait SkewTuneBackend {
  def reportNextFetchIterator(taskId: Long, fetchIndex: Int)

  def reportBlockStatuses(taskID: Long, seq: Seq[(BlockId, Byte)], newTaskId: Option[Long] = None, size: Option[Long])

  def reportTaskFinished(taskID: Long)

  def registerNewTask(taskID: Long, executorId: String, seq: Seq[SkewTuneBlockInfo])

  //9.3 SkewTuneAdd
  def reportTaskComputeSpeed(taskId: Long, executorId: String, speed: Float)

  def reportBlockDownloadSpeed(fromExecutor: String, toExecutor: String, speed: Float)
}

private[spark] class SkewTuneMaster(val taskSetManager: TaskSetManager,val conf: SparkConf,
                                    val networkSpeed: mutable.HashMap[(String, String), Float]) extends Logging {
  //9.25 SkewTuneAdd
  var maxFetchIndex = 0
  var currentFetchIndex = 0
  val tmpActiveTasks = new mutable.HashMap[Long, SkewTuneTaskInfo]
  var nextTaskRunningOrFinished = 0

  //activeTask接受注册时blockMap为空的task，因为未来可能给它分配skewtuneTask
  val activeTasks = new mutable.HashMap[Long, SkewTuneTaskInfo]
  //sbt:error private class escapes its defining scope。因为SkewTuneTaskInfo是private，但是activeTask是public，所以内部类逃逸了
  //9.19 SkewTuneAdd
  val isRegistered = new mutable.HashMap[Long, Boolean]()
  val demonTasks = new mutable.HashSet[Long]()
  var taskFinishedOrRunning = 0
  var times_communicate = 0
  var times_compute = 0
  var overhead_communicate: Long = 0
  var overhead_compute: Long = 0

  //9.5 SkewTuneAdd 在taskset级别上记录某个executor上面的compute速度
  val computeSpeed = new mutable.HashMap[String, Float]()
  private val TIME_SCHEDULE_COST_OF_FETCH = 0
  private val TIME_SCHEDULE_COST_OF_RESULT = 0

  val onlyUseSize = conf.getBoolean("spark.skewtune.onlyUseSize",false)
  val speedRatio = conf.getDouble("spark.skewtune.speedRatio",0.5).toFloat
  val advanced = conf.getBoolean("spark.skewtune.advanced",false)
  val useResult2Fetch = conf.getBoolean("spark.skewtune.advanced.useResult2Fetch",true)
  val interval = conf.getInt("spark.skewtune.timeInterval",100)

  logInfo(s"TaskSet ${taskSetManager.name} InitMaster onlyUseSize $onlyUseSize speedRatio $speedRatio advanced $advanced useResult2Fetch $useResult2Fetch interbal $interval" )

  @deprecated
  private def costOfSchedule(blockInfo: SkewTuneBlockInfo, oldExecutor: String, newExecutor: String): Int = {
    val blockAddress = blockInfo.blockManagerId.executorId
    val avgSpeed = computeSpeed.values.sum / computeSpeed.size
    val avgSpeedNetwork = networkSpeed.values.sum / networkSpeed.size
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
      val c1Speed = if (computeSpeed.isDefinedAt(oldExecutor)) computeSpeed(oldExecutor) else avgSpeed
      val c2Speed = if (computeSpeed.isDefinedAt(newExecutor)) computeSpeed(newExecutor) else avgSpeed
      val costDiff = TIME_SCHEDULE_COST_OF_FETCH + blockInfo.blockSize / networkSpeed.getOrElse((blockAddress, newExecutor), avgSpeedNetwork)
      -blockInfo.blockSize / networkSpeed.getOrElse((blockAddress, oldExecutor), avgSpeedNetwork) +
        blockInfo.blockSize / c2Speed - blockInfo.blockSize / c1Speed
      costDiff.toInt
    }
  }

  //9.5 对一个block Seq统计所需完成时间，不考虑已经used的block 和 maxFlightInKb对Fetching的限制
  //9.14 LOCAL_FETCHED/REMOTE_FETCHED 考虑计算速度。REMOTE_FETCH_WAITING/REMOTE_FETCHING 考虑计算速度和下载速度。其他0
  private def timeBySeq(blockInfo: Seq[SkewTuneBlockInfo], onWhichExecutor: String): Long = {
    val useTime = blockInfo.filter(_.blockManagerId.executorId != onWhichExecutor)
      .forall(b => networkSpeed.isDefinedAt(b.blockManagerId.executorId, onWhichExecutor))
    if (useTime && !onlyUseSize) {
      val avgSpeed = computeSpeed.values.sum / computeSpeed.size
      val timeBasedOnSpeed = blockInfo.map(info => {
        //9.25 SkewTuneAdd reportComputeSpeed被lock特性阻塞了
        if ((info.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0)
          (info.blockSize / computeSpeed.getOrElse(onWhichExecutor, avgSpeed)).toLong
        else if ((info.blockState & (SkewTuneBlockStatus.REMOTE_FETCH_WAITING | SkewTuneBlockStatus.REMOTE_FETCHING)) != 0)
          //9.28 bugFix fetch的block就在onWhichExecutor上，则网速为最大
          ({if(info.blockManagerId.executorId != onWhichExecutor) info.blockSize / networkSpeed(info.blockManagerId.executorId, onWhichExecutor) else 0} +
            info.blockSize / computeSpeed.getOrElse(onWhichExecutor, avgSpeed)).toLong
        else
          0
      }).sum
      if(speedRatio >= 0 && speedRatio <= 1) {
        val size = blockInfo.map(_.blockSize).sum
        (size * (1 - speedRatio) + timeBasedOnSpeed * speedRatio).toLong
      }else
        timeBasedOnSpeed
    } else
      blockInfo.map(_.blockSize).sum
  }

  def getMinSizeTaskId: Long ={
    activeTasks.minBy(task => timeBySeq(task._2.blockMap.values.toSeq, task._2.executorId))._1
  }

  //9.5 加入了计算速度和下载速度作为调度block的cost基础
  def computerAndSplit(isLastTask: Boolean, schedulerBackend: CoarseGrainedSchedulerBackend = null):
  Option[(Seq[RemoveFetchCommand], Seq[RemoveAndAddResultCommand], Seq[RemoveResultAndAddFetchCommand], Seq[RemoveResultAndAddResultCommand], Long)] = {
    logInfo(s"Master on taskSet ${taskSetManager.name} computerAndSplit with COST. activeTaskNumber: ${activeTasks.keySet}")
    val start_time = System.currentTimeMillis()
    val transferFetches = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResults = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResultToFetches = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    val transferResultToResults = new mutable.HashMap[(SkewTuneTaskInfo, SkewTuneTaskInfo), mutable.Buffer[SkewTuneBlockInfo]]
    //9.5 按照完成任务所需时间从大到小排序
    val sortedTasks = new ArrayBuffer[SkewTuneTaskInfo] ++= activeTasks.values.toList
      .sortBy(task => timeBySeq(task.blockMap.values.toSeq, task.executorId)).reverse

    logInfo(s"\t\t\t[%%%]sortTasks: ${sortedTasks.map(t => (t.taskId, t.executorId))}")
    val taskToSplit = if (sortedTasks.length >= 2) Some(sortedTasks.remove(0)) else None

    //8.31 taskToSpilt必须有block时才可以分割
    if (taskToSplit.isDefined && taskToSplit.get.blockMap.nonEmpty) {
      //9.5 这三种status的block可以被调度，任何时候used的block不在考虑范围内
      val blocksToSchedule =  new mutable.HashMap[BlockId,SkewTuneBlockInfo]() ++= taskToSplit.get.blockMap.filter(blockInfo =>
        (blockInfo._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED
          | SkewTuneBlockStatus.REMOTE_FETCH_WAITING)) != 0 && !blockInfo._2.isMoved)
        //9.26
        //.filter(_._2.isMoved == false)//.toList
        //9.27 block sort
        //.sortBy(_._2.blockSize).reverse
      /*val blockTimeCostNotSchedulable = timeBySeq(taskToSplit.get.blockMap.values
        .filter(_.blockState == SkewTuneBlockStatus.REMOTE_FETCHING).toSeq, taskToSplit.get.executorId)*/
      //if (isLastTask)
      //maximumFairnessSchedule() //sbt :Error :forward reference extends over definition of value fetchCommands。在fetchCommands定义之前就引用了它
      val fetchCommands = new ArrayBuffer[RemoveFetchCommand]()
      val resultCommands = new ArrayBuffer[RemoveAndAddResultCommand]()
      val resultToFetchCommands = new ArrayBuffer[RemoveResultAndAddFetchCommand]()
      val resultToResultCommands = new ArrayBuffer[RemoveResultAndAddResultCommand]()
      var minSizeTaskId: Option[Long] = None

      if (sortedTasks.nonEmpty) {
        val secondTask = sortedTasks.head
        var firstTaskTime = timeBySeq(taskToSplit.get.blockMap.values.toSeq, taskToSplit.get.executorId)
        var maxTimeCostLine = timeBySeq(secondTask.blockMap.values.toSeq, secondTask.executorId)
        minimumCostSchedule()

        //9.6 对每个block,找到调度开销最小的目标task列表，检查task是否还能添加新的block进去直到找到一个可行task
        def minimumCostSchedule(): Unit = {
          val tasksToAddBlock = sortedTasks
          logInfo(s"\t\t\t[%%%]network ${networkSpeed.map(n => {n._1._1 + "to" + n._1._2 + "=" + n._2})}")
          logInfo(s"\t\t\t[%%%]compute $computeSpeed")
          logInfo(s"\t\t\t[%%%]taskToSplit ${taskToSplit.get.taskId} on ${taskToSplit.get.executorId}")
          val taskTime = new mutable.HashMap[Long, Long]() ++= tasksToAddBlock.map(info => (info.taskId, timeBySeq(info.blockMap.values.toSeq, info.executorId)))
          logInfo(s"\t\t\t[%%%]firstTaskTime: $firstTaskTime maxTimeCostLine $maxTimeCostLine taskTime $taskTime")
          for (block <- blocksToSchedule if firstTaskTime > maxTimeCostLine) {
            logInfo(s"\t\t\t[%%%]Start Loop maxTimeCostLine $maxTimeCostLine firstTaskTime $firstTaskTime")
            logInfo(s"\t\t\t[%%%]now to process block ${block._1} on ${block._2.blockManagerId.executorId} with size ${block._2.blockSize} (${block._2.blockSize/1024F/1024}MB})")

            if ((block._2.blockState & (SkewTuneBlockStatus.LOCAL_FETCHED | SkewTuneBlockStatus.REMOTE_FETCHED)) != 0) {
              logInfo(s"\t\t\t[%%%]block ${block._1} status = Local_Fetched or Remote_Fetched")
              //9.26 只在executor内部转移
              if (!advanced) {
                val taskOp = sortedTasks.filter(task => (task.executorId == taskToSplit.get.executorId)
                  //9.26 bugFix 加上 = ，防止候选task只有一个导致不调度
                  && (maxTimeCostLine - taskTime(task.taskId) >= 0))
                  .sortBy(task => {
                  /*val cost = costOfSchedule(block._2, taskToSplit.get.executorId, task.executorId)
                  logInfo(s"\t\t\t[%%%]costOfSchedule = $cost (${taskToSplit.get.taskId} on ${taskToSplit.get.executorId} -> ${task.taskId} on ${task.executorId})")
                  cost*/
                  //9.26 按候选Task剩余时间从小到大，因为同一个executor上，所以不算block在不同task上的时间差异了，但下面要算总时间
                  taskTime.getOrElse(task.taskId, Long.MaxValue)
                })
                if (taskOp.nonEmpty) {
                  val task = taskOp.head
                  //9.24 transferResult必须在同一个executor中，如果不满足，就会跳过该block
                  logInfo(s"\t\t\t[%%%] find valid task ${task.taskId} on ${task.executorId}")
                  transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                  firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                  val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                  taskTime.update(task.taskId, newTime)
                  taskTime.update(taskToSplit.get.taskId,firstTaskTime)
                  maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
                }
              } else {
                //9.26 在全部executor中转移
                //9.26 SkewTuneAdd 不能在同一个Executor中转移Result，就Result2Fetch，
                // 还有一种思路，直接转移Result，但如果块较大，可能严重拖慢Driver响应时间，
                // 那吧目的executor的actor对象发过来 ？？？
                if(useResult2Fetch) {
                  //9.26 sortedTasks本来就是有序的
                  val taskOp = sortedTasks.filter(task => maxTimeCostLine - taskTime(task.taskId) >= 0).toList
                    //9.26 转移到其他executor时，排序按完成时间先后排序
                    .sortBy(task => taskTime.getOrElse(task.taskId, Long.MaxValue))
                  if (taskOp.nonEmpty) {
                    val task = taskOp.head
                    logInfo(s"\t\t\t[%%%Advanced R2F] find valid task ${task.taskId} on ${task.executorId}")
                    if (task.executorId != taskToSplit.get.executorId)
                      transferResultToFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                    else
                      transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                    firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                    val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                    taskTime.update(task.taskId, newTime)
                    taskTime.update(taskToSplit.get.taskId,firstTaskTime)
                    maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
                  }
                } else {
                  val taskOp = sortedTasks.toList.sortBy(task => taskTime.getOrElse(task.taskId, Long.MaxValue))
                  if (taskOp.nonEmpty) {
                    val task = taskOp.head
                    logInfo(s"\t\t\t[%%%Advanced R2R] find valid task ${task.taskId} on ${task.executorId}")
                    if (task.executorId != taskToSplit.get.executorId)
                      transferResultToResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                    else
                      transferResults.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                    firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                    val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                    taskTime.update(task.taskId, newTime)
                    taskTime.update(taskToSplit.get.taskId,firstTaskTime)
                    maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
                  }
                }
              }
            } else {
              logInfo(s"\t\t\t[%%%]block ${block._1} status = Fetch_Waiting")
              val taskOp = sortedTasks.filter(info => maxTimeCostLine - taskTime(info.taskId) >= 0).toList
                .sortBy(task => {
                /*val cost = costOfSchedule(block._2, taskToSplit.get.executorId, task.executorId)
                logInfo(s"\t\t\t[%%%]costOfSchedule = $cost (${taskToSplit.get.taskId} on ${taskToSplit.get.executorId} -> ${task.taskId} on ${task.executorId})")
                cost*/
                //9.26
                taskTime.getOrElse(task.taskId, Long.MaxValue) + timeBySeq(Seq(block._2), task.executorId)
              })
              if (taskOp.nonEmpty) {
                val task = taskOp.head
                logInfo(s"\t\t\t[%%%]find valid task ${task.taskId} on ${task.executorId}")
                transferFetches.getOrElseUpdate((taskToSplit.get, task), new ArrayBuffer[SkewTuneBlockInfo]()) += block._2
                firstTaskTime -= timeBySeq(Seq(block._2), taskToSplit.get.executorId)
                val newTime = taskTime.get(task.taskId).get + timeBySeq(Seq(block._2), task.executorId)
                taskTime.update(task.taskId, newTime)
                taskTime.update(taskToSplit.get.taskId,firstTaskTime)
                maxTimeCostLine = Math.max(maxTimeCostLine, newTime.toInt)
              }
            }
            blocksToSchedule -= block._1
            logInfo(s"\t\t\t[%%%]End Loop maxTimeCostLine $maxTimeCostLine firstTaskTime $firstTaskTime\n")
          }
          logInfo(s"\t\t\t[%%%]taskTime $taskTime \n")
          minSizeTaskId = Some(taskTime.minBy(_._2)._1)
          logInfo(s"\t\tminimumCostSchedule : Fetches : ${transferFetches.size} , Results : ${transferResults.size} , R2F : ${transferResultToFetches.size}")
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
      //9.26
      if (advanced) {
        if (useResult2Fetch) {
          transferResultToFetches.foreach(info => {
            resultToFetchCommands += RemoveResultAndAddFetchCommand(info._2.map(_.blockId), info._1._1.taskId, info._1._2.taskId, info._1._2.executorId)
          })
        }
        else {
          transferResultToResults.foreach(info => {
            resultToResultCommands += RemoveResultAndAddResultCommand(info._2.map(_.blockId), info._1._1.taskId, info._1._2.taskId, schedulerBackend.executorDataMap(info._1._2.executorId).executorEndpoint)
          })
        }
      }

      val time_used = System.currentTimeMillis() - start_time
      if (fetchCommands.nonEmpty || resultCommands.nonEmpty || resultToFetchCommands.nonEmpty || resultToResultCommands.nonEmpty) {
        logInfo(s"\t\tcomputerAndSplit : fetchCommands $fetchCommands , resultCommands : $resultCommands " +
          s", resultToFetchCommands :$resultToFetchCommands , resultToResultCommands : $resultToResultCommands " +
          s". time : $time_used ms")
        Some((fetchCommands, resultCommands, resultToFetchCommands, resultToResultCommands, minSizeTaskId.get))
      } else {
        logInfo(s"\t\tcomputerAndSplit Terminate: fetchCommands / resultCommands all empty . time : $time_used ms")
        None
      }
    } else {
      logInfo(s"\t\tcomputerAndSplit Terminate: taskToSplit is empty or its blockMap is empty.")
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

  //9.26 bugFix : taskID == newTaskID when after removeAndAddResult report
  def reportBlockStatuses(taskId: Long, seq: Seq[(BlockId, Byte)], oldTaskId: Option[Long] = None, size: Option[Long]): Unit = {
    if (oldTaskId.isDefined)
      logInfo(s"on taskSet ${taskSetManager.name}:reportBlockStatuses : task $taskId/$currentFetchIndex, seq $seq, newTaskID: $oldTaskId, size: ${size.getOrElse(0L) / 1024F / 1024} MB")
    if (activeTasks.contains(taskId)) {
      val blockMapInNewTask = activeTasks.get(taskId).get.blockMap

      if (oldTaskId.isEmpty) {
        for ((blockId, newBlockState) <- seq if blockMapInNewTask.contains(blockId)) {
          val skewTuneBlockInfo = blockMapInNewTask(blockId)
          skewTuneBlockInfo.blockState = if (newBlockState > 0) newBlockState else skewTuneBlockInfo.blockState
        }
      } else if (oldTaskId.get != taskId && activeTasks.contains(taskId)) {
        val blockMapInOldTask: mutable.Map[BlockId, SkewTuneBlockInfo] = activeTasks(oldTaskId.get).blockMap
        for ((blockId, newBlockState) <- seq if blockMapInOldTask.contains(blockId)) {
          val skewTuneBlockInfo = blockMapInOldTask(blockId)
          skewTuneBlockInfo.blockState = if (newBlockState > 0) newBlockState else skewTuneBlockInfo.blockState
          //9.26 bugFix 防止某个block反复在两个Task之间来回
          skewTuneBlockInfo.isMoved = true
          blockMapInNewTask += ((blockId, skewTuneBlockInfo))
          blockMapInOldTask -= blockId
        }
      } else
        logInfo(s"on taskSet ${taskSetManager.name}:reportBlockStatuses : error task $taskId not exist . oldTaskId $oldTaskId")
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
}

private[spark] class SkewTuneTaskInfo(val taskId: Long, val executorId: String, val blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]) extends Serializable {
  override def toString = s"SkewTuneTask_$taskId :mapSize_${blockMap.size}_onExecutor_$executorId"
}

private[spark] object SkewTuneTaskInfo {
  def apply(taskId: Long, executorId: String, blockMap: mutable.Map[BlockId, SkewTuneBlockInfo]): SkewTuneTaskInfo = {
    new SkewTuneTaskInfo(taskId, executorId, blockMap)
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
  var isMoved = false

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
