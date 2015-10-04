/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.storage.{SkewTuneTaskInfo, SkewTuneBlockInfo, BlockId}
import org.apache.spark.util.{AkkaUtils, SerializableBuffer, ThreadUtils, Utils}
import org.apache.spark.{ExecutorAllocationClient, Logging, SparkEnv, SparkException, TaskState}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging {
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  var totalRegisteredExecutors = new AtomicInteger(0)
  val conf = scheduler.sc.conf
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")
  val createTime = System.currentTimeMillis()

  val executorDataMap = new HashMap[String, ExecutorData] //9.26 change to public

  // Number of executors requested from the cluster manager that have not registered yet
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet
  private val executorsPendingToRemove = new HashSet[String]

  //8.24 SkewTuneAdd : hasSkewTuneTaskRunByExecutorId 记录executor是否运行过task
  /*val master = new SkewTuneMaster*/
  //sbt：error:values cannot be volatile。因为volatile表示便以其不能确定岂会发生变化，与val矛盾
  /*val hasSkewTuneTaskRunByExecutorId = new mutable.HashMap[String, Boolean]*/

  //9.5 SkewTuneAdd : 记录网速（executorA -> executorB) 每个block下载完成报告一次
  val networkSpeed = new mutable.HashMap[(String, String), Float]()
  //9.26
  val schedulerBackendInstance = this

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    override protected def log = CoarseGrainedSchedulerBackend.this.log

    private val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }

    override def receive: PartialFunction[Any, Unit] = {
      //8.5 TaskSchedulerImpl.statusUpdate更新task状态
      // ????makeOffers(executorId)中又有launchTasks，但statusUpdate(taskId, state, data.value)中也有makeOffers()
      case StatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              makeOffers(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                "from unknown executor $sender with ID $executorId")
          }
        }

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }

      //8.24 SkewTuneAdd : Executor到Master的消息处理
      case TransferRemovedFetch(nextExecutorId, nextTaskId, returnSeq, fromTaskId) =>
        executorDataMap.get(nextExecutorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(AddFetchCommand(nextTaskId, returnSeq, fromTaskId))
          case None =>
            logWarning(s"Attempted to TransferRemovedFetch to Task $nextTaskId for unknown executor $nextExecutorId.")
        }

      case ReportBlockStatuses(taskID, seq, oldTaskId, size) =>
        //logInfo(s"Master : Received Command ReportBlockStatuses for task $taskID (to new Task $newTaskId)")
        val time = System.currentTimeMillis()
        if(scheduler.taskIdToTaskSetId.get(taskID).isDefined
            && scheduler.activeTaskSets.get(scheduler.taskIdToTaskSetId(taskID)).isDefined){
          val master = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskID)).master
          master.reportBlockStatuses(taskID, seq, oldTaskId, size)
          master.overhead_communicate += System.currentTimeMillis() - time
          master.times_communicate += 1
        }

      case ReportTaskFinished(taskID: Long) =>
        //logInfo(s"Master : Received Command ReportTaskFinished for task $taskID")
        val time = System.currentTimeMillis()
        val taskset = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskID))
        val master = taskset.master
        master.reportTaskFinished(taskID)
        //9.25 SkewTuneAdd
        master.tmpActiveTasks -= taskID
        //9.18 SkewTuneAdd Release unlock task
        /*if(taskset.unlockedTaskId.isDefined && taskset.unlockedTaskId.get == taskID)
          taskset.unlockedTaskId = None*/
        //9.19
        if(master.demonTasks.contains(taskID))
          master.demonTasks -= taskID
        //9.29
        if(master.isLastTask && master.currentFetchIndex == master.maxFetchIndex){
          logInfo(s"[LastTask2] on taskSetManager ${master.taskSetManager.name}: When Other Task Finished . UnLock Remaining Tasks ${master.activeTasks.keySet}")
          master.activeTasks.keySet.foreach(id => executorDataMap(scheduler.taskIdToExecutorId(id)).executorEndpoint.send(UnlockTask(id, master.currentFetchIndex)))
        }
        master.overhead_communicate += System.currentTimeMillis() - time
        master.times_communicate += 1

      case ReportNextFetchIterator(taskId,fetchIndex) =>
        //9.25 SkewTuneAdd 多段锁的同步
        val time = System.currentTimeMillis()
        val master = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskId)).master
        //if(fetchIndex > master.maxFetchIndex) {
        logInfo(s"[&] on taskSetManager ${master.taskSetManager.name}: taskId: $taskId reportNextIterator $fetchIndex")
        if(!master.demonTasks.contains(taskId)) {
          executorDataMap(scheduler.taskIdToExecutorId(taskId)).executorEndpoint.send(UnlockTask(taskId,fetchIndex))
          master.nextTaskRunningOrFinished += 1
          logInfo(s"[&] on taskSetManager ${master.taskSetManager.name}: UnLock task $taskId/$fetchIndex")
        }
        //}
        master.maxFetchIndex = Math.max(master.maxFetchIndex,fetchIndex)
        master.overhead_communicate += System.currentTimeMillis() - time
        master.times_communicate += 1

      case RegisterNewTask(taskId, executorId, seq) =>
        //logInfo(s"Master : Received Command RegisterNewTask for task $taskId on Executor $executorId")
        var time = System.currentTimeMillis()
        val master = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskId)).master
        //9.19 SkewTuneAdd
        if(master.isRegistered.get(taskId).isEmpty){
          master.registerNewTask(taskId, executorId, seq)
          val demonTasks = master.demonTasks
          //val hasSkewTuneTaskRunByExecutor = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskId)).hasSkewTuneTaskRunByExecutor
          val availableMaxTaskNumberConcurrent: Int = executorDataMap.map(_._2.totalCores).sum
          //8.24 判断SkewTune重新分配blocks的触发条件:该taskset中正在运行的executor上非第一个task注册上来时调度。
          // 如果是taakset的最后一个task需要额外的调度
          //8.30 bug: 所有skewtuneblockInfo中的size都为0，但是hadoopRDD中确实split了16个，也看到了16个offset
          //if (hasSkewTuneTaskRunByExecutorId.contains(executorId) && hasSkewTuneTaskRunByExecutorId(executorId)) {
          //9.12 SkewTuneAdd 触发条件：还剩余的task数量 <= 可用的executor数量，并且可调度的block的数量小于一定数量
          val taskset = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskId))
          //9.19 SkewTuneAdd
          val isLastTask = master.taskFinishedOrRunning == taskset.tasks.length
          master.isLastTask = isLastTask
          logInfo(s"on taskSetManager ${master.taskSetManager.name}: taskId: $taskId isLastTask $isLastTask fetchIndex ${master.currentFetchIndex} ,runningorFinished: ${master.taskFinishedOrRunning} taskSize: ${taskset.tasks.length}")
          master.overhead_communicate += System.currentTimeMillis() - time
          master.times_communicate += 1
          time = System.currentTimeMillis()
          if (availableMaxTaskNumberConcurrent > 0 && master.taskFinishedOrRunning  >= availableMaxTaskNumberConcurrent
          /*taskset.tasksSuccessful >= availableMaxTaskNumberConcurrent
            && taskset.successful.length - taskset.tasksSuccessful <= availableMaxTaskNumberConcurrent*/) {
            logInfo(s"on taskSetManager ${master.taskSetManager.name}: Start SkewTune Split . DemonTasks $demonTasks")
            /*//9.18 SkewTuneAdd lock the tasks
            if(isLastTask){
              master.activeTasks.foreach(info => {
                executorDataMap(scheduler.taskIdToExecutorId(info._1)).executorEndpoint.send(UnlockTask(info._1))
              })
              logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: isLastTask so unlockAllTask ")
            }else{
              taskset.unlockedTaskId match {
                case None =>
                  //9.18 SkewTuneAdd lock other tasks which active 默认上一个unlocktask结束时下一个递补
                  val toLockTasks = master.activeTasks.filter(_._1 != taskId)
                  toLockTasks.foreach(info => {
                    executorDataMap(scheduler.taskIdToExecutorId(info._1)).executorEndpoint.send(LockTask(info._1))
                  })
                  taskset.unlockedTaskId = Some(taskId)
                  logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: unlockTask is None. to Lock Tasks ${toLockTasks.keys} and unlockTask $taskId")
                case _ =>
                  //9.18 SkewTuneAdd lock this task
                  executorDataMap(scheduler.taskIdToExecutorId(taskId)).executorEndpoint.send(LockTask(taskId))
                  logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: unlockTask is Some. to Lock Tasks $taskId")
              }
            }*/
            val startTime = System.currentTimeMillis()
            var smallSizeTaskIdOp: Option[Long] = None
            val commandsOption = master.computerAndSplit(isLastTask , schedulerBackend = schedulerBackendInstance)
            commandsOption match {
              case Some((fetchCommands, resultCommands,fetchingCommands, result2FetchCommands,result2ResultCommands,smallSizeTaskId)) =>
                smallSizeTaskIdOp = Some(smallSizeTaskId)
                for (command <- fetchCommands if scheduler.taskIdToExecutorId.contains(command.taskId)) {
                  executorDataMap(scheduler.taskIdToExecutorId(command.taskId)).executorEndpoint.send(command)
                }
                for (command <- resultCommands if scheduler.taskIdToExecutorId.contains(command.fromTaskId)) {
                  executorDataMap(scheduler.taskIdToExecutorId(command.fromTaskId)).executorEndpoint.send(command)
                }
                //9.30
                if(master.transferFetchingBlocks){
                  for (command <- fetchingCommands if scheduler.taskIdToExecutorId.contains(command.taskId)) {
                    executorDataMap(scheduler.taskIdToExecutorId(command.taskId)).executorEndpoint.send(command)
                  }
                }
                //9.26
                if(master.advanced) {
                  for (command <- result2FetchCommands if scheduler.taskIdToExecutorId.contains(command.fromTaskId)) {
                    executorDataMap(scheduler.taskIdToExecutorId(command.fromTaskId)).executorEndpoint.send(command)
                  }
                  for (command <- result2ResultCommands if scheduler.taskIdToExecutorId.contains(command.fromTaskId)) {
                    executorDataMap(scheduler.taskIdToExecutorId(command.fromTaskId)).executorEndpoint.send(command)
                  }
                }
                logInfo(s"\ton taskSetManager ${master.taskSetManager.name}:Valid SkewTuneSplit. TimeCost: ${System.currentTimeMillis - startTime} ms")

              case None =>
                logInfo(s"\ton taskSetManager ${master.taskSetManager.name}:Terminate because ActiveTasks.length < 2 or 3. TimeCost: ${System.currentTimeMillis - startTime} ms ")
            }
            //9.26 lastTask时不再unlock
            if(!isLastTask) {
              //9.23 最先完成的task可以被unlock了，分为当前task是新task还是之前被lock的task两种情况
              val smallSizeTaskId = if(smallSizeTaskIdOp.isDefined) smallSizeTaskIdOp.get else master.getMinSizeTaskId
              logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: smallest size task $smallSizeTaskId/${master.currentFetchIndex} task $taskId")
              if(demonTasks.size < availableMaxTaskNumberConcurrent - 1) {
                if (taskId != smallSizeTaskId)
                  demonTasks += taskId
              }
              else{
                executorDataMap(scheduler.taskIdToExecutorId(smallSizeTaskId)).executorEndpoint.send(UnlockTask(smallSizeTaskId,master.currentFetchIndex))
                logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: to Unlock smallest size task $smallSizeTaskId/${master.currentFetchIndex} ")
                if (smallSizeTaskId != taskId) {
                  demonTasks -= smallSizeTaskId
                  demonTasks += taskId
                }
              }
            }
            master.times_compute += 1
          } else{
            logInfo(s"\ton taskSetManager ${master.taskSetManager.name}: demonTask $demonTasks maxSize = ${availableMaxTaskNumberConcurrent-1}")
            if(demonTasks.size < availableMaxTaskNumberConcurrent -1 ) {
              demonTasks += taskId
            }
          }
          if(isLastTask) {
            master.activeTasks.keySet.foreach(id =>
              {
                if(master.currentFetchIndex == master.maxFetchIndex && master.interval > 0)
                  Thread.sleep(master.interval)
                executorDataMap(scheduler.taskIdToExecutorId(id)).executorEndpoint.send(UnlockTask(id,master.currentFetchIndex))
              })
            logInfo(s"[LastTask] on taskSetManager ${master.taskSetManager.name}: UnLock Remaining Tasks ${master.activeTasks.keySet}")
            if(master.currentFetchIndex < master.maxFetchIndex) {
              //9.25 SkewTuneAdd 前一个Lock阶段的最后一个Task时，重新初始化Master中变量
              //9.26 bugFix reportNextFetchIterator时都Unlock了
              //master.demonTasks.clear()
              master.activeTasks.clear()
              master.taskFinishedOrRunning = 0
              master.isRegistered.clear()
              logInfo(s"[LastTask] on taskSetManager ${master.taskSetManager.name}: LockPhase ${master.currentFetchIndex} End")
              master.currentFetchIndex += 1
              //9.26 bugFix demonTask从上一个Lock Phase继承下来
              //master.demonTasks ++= master.tmpActiveTasks.keys
              master.activeTasks ++= master.tmpActiveTasks
              master.isRegistered ++= master.tmpActiveTasks.map(t => (t._1, true))
              master.taskFinishedOrRunning += (master.tmpActiveTasks.size + master.nextTaskRunningOrFinished)
              master.tmpActiveTasks.clear()
              master.nextTaskRunningOrFinished = 0
              logInfo(s"[LastTask] on taskSetManager ${master.taskSetManager.name}: new ActiveTasks ${master.activeTasks.keySet}")
            }
          }
          master.overhead_compute += System.currentTimeMillis() - time
          //hasSkewTuneTaskRunByExecutor(executorId) = true
          assert(demonTasks.size <= availableMaxTaskNumberConcurrent - 1 )
        }else{
          //9.25 SkewTuneAdd 前一个Lock阶段中来了后一个Lock阶段的Task，先缓存起来
          val blockMap = new mutable.HashMap[BlockId, SkewTuneBlockInfo]() ++= seq.map(info => (info.blockId, info))
          master.tmpActiveTasks += ((taskId, SkewTuneTaskInfo(taskId, executorId, blockMap)))
          logInfo(s"on taskSetManager ${master.taskSetManager.name}: tasks $taskId/${master.currentFetchIndex + 1} to tmpActiveTasks")
        }

      //9.5 SkewTuneAdd
      case ReportTaskComputeSpeed(taskId, executorId, speed) =>
        //logInfo(s"Master : Received Command ReportTaskComputeSpeed for task $taskId on Executor $executorId with speed $speed byte/ms")
        val time = System.currentTimeMillis()
        val master = scheduler.activeTaskSets(scheduler.taskIdToTaskSetId(taskId)).master
        master.reportTaskComputerSpeed(taskId, executorId, speed)
        master.overhead_communicate += System.currentTimeMillis() - time
        master.times_communicate += 1

      case ReportBlockDownloadSpeed(fromExecutor, toExecutor, speed) =>
        //logInfo(s"Received Command ReportBlockDownloadSpeed from Executor $fromExecutor to Executor $toExecutor with speed $speed byte/ms")
        //9.25 SkewTuneAdd 经常出现speed a-b 4000，b-a 0.06 ,太小的用平均值代替
        if(speed >= 10){
          val lastSpeed = networkSpeed.getOrElseUpdate((fromExecutor, toExecutor), speed)
          networkSpeed += (((fromExecutor, toExecutor), (lastSpeed + speed) / 2))
        }else if (networkSpeed.isDefinedAt((toExecutor, fromExecutor)) && networkSpeed((toExecutor,fromExecutor)) > 10)
          networkSpeed += (((fromExecutor, toExecutor), networkSpeed((toExecutor,fromExecutor))))

    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          logInfo("Registered executor: " + executorRef + " with ID " + executorId)
          context.reply(RegisteredExecutor)
          addressToExecutorId(executorRef.address) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val (host, _) = Utils.parseHostPort(hostPort)
          val data = new ExecutorData(executorRef, executorRef.address, host, cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          makeOffers()
        }

      case StopDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        context.reply(true)
        //8.26 SkewTuneAdd
        /*for (taskset <- scheduler.activeTaskSets.values) {
          taskset.hasSkewTuneTaskRunByExecutor.remove(executorId)
        }*/

      case RetrieveSparkProps =>
        context.reply(sparkProperties)
    }

    // Make fake resource offers on all executors
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach(removeExecutor(_,
        "remote Rpc client disassociated"))
    }

    // Make fake resource offers on just one executor
    def makeOffers(executorId: String) {
      val executorData = executorDataMap(executorId)
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      logDebug(s"[!!!!] Tasks ${tasks.flatten}")
      for (task <- tasks.flatten) {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val serializedTask = ser.serialize(task)
        //8.24 如果序列化后的task大小超过akka单帧大小，则终止该taskSet
        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
          val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
          scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                "spark.akka.frameSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                AkkaUtils.reservedSizeBytes)
              taskSet.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          //8.11  executorDataMap(task.executorId)获得task对应的executor
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK
          logDebug(s"[!!!!] Send LaunchTasks ${task.taskId}")
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: String): Unit = {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingToRemove -= executorId
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, SlaveLost(reason))
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason))
        case None => logError(s"Asked to remove non-existent executor $executorId")
      }
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    driverEndpoint = rpcEnv.setupEndpoint(
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME, new DriverEndpoint(rpcEnv, properties))
  }

  def stopExecutors() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithRetry[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askWithRetry[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  //8.6 这个类是在master上的，而
  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  // Called by subclasses when notified of a lost worker
  def removeExecutor(executorId: String, reason: String) {
    try {
      driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver endpoint", e)
    }
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")

      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  def numExistingExecutors: Int = executorDataMap.size

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = synchronized {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
          s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")
    logDebug(s"Number of pending executors is now $numPendingExecutors")
    numPendingExecutors += numAdditionalExecutors
    // Account for executors pending to be added or removed
    val newTotal = numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
    doRequestTotalExecutors(newTotal)
  }

  /**
   * Express a preference to the cluster manager for a given total number of executors. This can
   * result in canceling pending requests or filing additional requests.
   * @return whether the request is acknowledged.
   */
  final override def requestTotalExecutors(numExecutors: Int): Boolean = synchronized {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }
    numPendingExecutors =
      math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)
    doRequestTotalExecutors(numExecutors)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Boolean = false

  /**
   * Request that the cluster manager kill the specified executors.
   * Return whether the kill request is acknowledged.
   */
  final override def killExecutors(executorIds: Seq[String]): Boolean = synchronized {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")
    val filteredExecutorIds = new ArrayBuffer[String]
    executorIds.foreach { id =>
      if (executorDataMap.contains(id)) {
        filteredExecutorIds += id
      } else {
        logWarning(s"Executor to kill $id does not exist!")
      }
    }
    // Killing executors means effectively that we want less executors than before, so also update
    // the target number of executors to avoid having the backend allocate new ones.
    val newTotal = (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
      - filteredExecutorIds.size)
    doRequestTotalExecutors(newTotal)

    executorsPendingToRemove ++= filteredExecutorIds
    doKillExecutors(filteredExecutorIds)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * Return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Boolean = false

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
