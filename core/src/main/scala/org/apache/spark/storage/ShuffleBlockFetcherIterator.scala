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

package org.apache.spark.storage

import java.util.concurrent.LinkedBlockingQueue

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.{Logging, TaskContext, TaskContextImpl}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}
import scala.util.{Failure, Try}

//9.28
import scala.collection.JavaConversions._
/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, values) tuples so the caller can handle blocks in a
 * pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches to they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param serializer serializer used to deserialize the data.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
                                         context: TaskContext,
                                         shuffleClient: ShuffleClient,
                                         blockManager: BlockManager,
                                         blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
                                         serializer: Serializer,
                                         maxBytesInFlight: Long/*,
                                         lockDefault: Boolean = true*/)
  extends Iterator[(BlockId, Try[Iterator[Any]])] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0 //8.19 SkewTuneAdd，del private[this]，make it public

  /**
   * The number of blocks proccessed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0 //8.19 SkewTuneAdd，del private[this]，make it public

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]() //8.19 SkewTuneAdd，del private[this]，make it public

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]() //8.19 SkewTuneAdd，del private[this]，make it public

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  //8.17 所有Result队列
  private[this] val results = new LinkedBlockingQueue[FetchResult] //8.19 SkewTuneAdd，del private[this]，make it public

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  //8.17 next()中用到，当前处理的Result
  @volatile private[this] var currentResult: FetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] var fetchRequests = new Queue[FetchRequest] //8.19 SkewTuneAdd，del private[this]，make it public

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  val shuffleMetrics = context.taskMetrics.createShuffleReadMetricsForDependency() //9.10 SkewTuneAdd，del private[this]，make it public

  private[this] val serializerInstance: SerializerInstance = serializer.newInstance()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @volatile private[this] var isZombie = false

  //8.22 SkewTuneAdd，isTaskRegistered、reportToMasterCache保证在registerNewTask前不调用reportBlockStatus
  private[this] val worker = context.asInstanceOf[TaskContextImpl].skewTuneWorker
  @volatile private[this] var isTaskRegistered = false
  private[this] val reportToMasterCache = new ArrayBuffer[(BlockId, Byte)]()
  private[this] var bytesActualProcess: Long = 0
  //9.18 SkewTuneAdd
 // val taskLockStatus = worker.executorInstance.taskLockStatus
  val executorInstance = worker.executorInstance
  val taskId = worker.taskId
  @volatile var needLock = true /*if(taskLockStatus.isDefinedAt(taskId)) taskLockStatus(taskId) else lockDefault*/
  //9.25
  @volatile var isLocked = false
  var blockNumber = 0
  //9.26
  //var totalTime = 0L

  initialize()

  logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId} isLock $needLock " +
    s"with Size ${blocksByAddress.flatMap(_._2.map(_._2)).sum} to fetch $numBlocksToFetch blocks\n " +
    s"LocalBlocks : $localBlocks \n" +
    s"RemoteBlocks : $remoteBlocks \n" +
    s"BlockAddress : $blocksByAddress")

  //8.19 Master处传来的指令可能与Worker端的实际情况不同步，
  // 在函数内部判断如果状态不同步，就跳过执行了，
  // removeFetchRequests需要返回真实已移除的序列给Master， 再传给addFetchRequests，否则两个Task就可能会发出相同的FetchRequest
  //8.21 改变传入参数为blocksByAddress数组，在函数内构造FetchRequest
  def addFetchRequests(allBlocks: Seq[(BlockManagerId, Seq[(BlockId, Long)])],fromTaskId: Long): Unit = {
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    val remoteRequests = new ArrayBuffer[FetchRequest]
    val tmpFetch = new ArrayBuffer[(BlockId, SkewTuneBlockInfo)]()
    this.synchronized {
      for ((address, blockInfos) <- allBlocks) {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          curBlocks += ((blockId, size))
          numBlocksToFetch += 1
          curRequestSize += size
          if (curRequestSize >= targetRequestSize) {
            val newRequest = new FetchRequest(address, curBlocks)
            remoteRequests += newRequest
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
            //8.22 SkewTuneAdd，reportBlockStatuses
            val tmpInfos: Seq[(BlockId, SkewTuneBlockInfo)] = curBlocks.map(blockInfoInCur => {
              (blockInfoInCur._1, SkewTuneBlockInfo(blockInfoInCur._1, blockInfoInCur._2,
                address, SkewTuneBlockStatus.REMOTE_FETCH_WAITING /*, Option(newRequest), None)*/))
            }).filter(_._2.blockSize > 0)
            worker.blocks ++= tmpInfos
            //8.23 向Master报告block Status,block所属的taskid变化
            tmpFetch ++= tmpInfos
            //worker.reportBlockStatuses(tmpInfos.map(info => (info._1, 0x00.asInstanceOf[Byte])),Some(worker.taskId))
          }
        }
        if (curBlocks.nonEmpty) {
          val newRequest = new FetchRequest(address, curBlocks)
          remoteRequests += newRequest
          //8.22 SkewTuneAdd，reportBlockStatuses
          val tmpInfos: Seq[(BlockId, SkewTuneBlockInfo)] = curBlocks.map(blockInfoInCur => {
            (blockInfoInCur._1, SkewTuneBlockInfo(blockInfoInCur._1, blockInfoInCur._2,
              address, SkewTuneBlockStatus.REMOTE_FETCH_WAITING /*, Some(newRequest), None)*/))
          }).filter(_._2.blockSize > 0)
          worker.blocks ++= tmpInfos
          //8.23 向Master报告block Status，只在add时需要报告，remove时不需要
          tmpFetch ++= tmpInfos
          //worker.reportBlockStatuses(tmpInfos.map(info => (info._1, 0x00.asInstanceOf[Byte])), Some(worker.taskId))
        }
      }
      //9.27
      val tmpFetchRequests = fetchRequests.dequeueAll(i => true)
      fetchRequests ++= remoteRequests
      fetchRequests ++= tmpFetchRequests

      if(tmpFetch.nonEmpty)
        worker.reportBlockStatuses(tmpFetch.map(info => (info._1, 0x00.asInstanceOf[Byte])), Some(fromTaskId),Some(tmpFetch.map(_._2.blockSize).sum))
      if(needLock && isLocked)
        synchronized {
          this.notifyAll()
        }
    }
    logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId}。addFetchRequests ： $remoteRequests")
  }

  //8.22 在已有的Fetch的blocks数组中查找符合条件的blockId，检查block状态必须为wait_fetch
  def removeFetchRequests(blocksToRemove: Seq[(BlockManagerId, Seq[BlockId])]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    val returnResults = new ArrayBuffer[(BlockManagerId, Seq[(BlockId, Long)])]
    this.synchronized {
      for (blockToRemove <- blocksToRemove) {
        for (request <- fetchRequests if request.address == blockToRemove._1) {
          val tmpBlocksLeft = new ArrayBuffer[(BlockId, Long)] ++= request.blocks
          val tmpBlocksRemove = new ArrayBuffer[(BlockId, Long)]
          for ((blockId, size) <- request.blocks if blockToRemove._2.contains(blockId)
            && worker.blocks.contains(blockId)
            && worker.blocks(blockId).blockState == SkewTuneBlockStatus.REMOTE_FETCH_WAITING) {
            tmpBlocksLeft -= ((blockId, size))
            tmpBlocksRemove += ((blockId, size))
          }
          request.blocks = tmpBlocksLeft
          returnResults += ((request.address, tmpBlocksRemove))
        }
      }
      numBlocksToFetch -= returnResults.map(_._2.length).sum
      //8.22 SkewTuneAdd
      worker.blocks --= returnResults.flatMap(_._2.map(_._1))
    }
    //logInfo(s"ShuffleBlockFetchIterator on Executor ${blockManager.blockManagerId.executorId}。removeFetchRequests ： $returnResults")
    returnResults
  }

  //8.19 同理Woker和Master因为信息不同步，需要Worker自己做正确性检查：removeFetchResults必须返回真实的已移除序列。
  // 区别在于这两个函数都在一个Executor上执行
  def addFetchResults(resultsToAdd: Seq[(SkewTuneBlockInfo, SuccessFetchResult)], fromTaskId: Long): Unit = {
    var totalBlocksToAdd = 0
    val tmpResults: Seq[BlockId] = results.toArray.filter(_.isInstanceOf[SuccessFetchResult])
      .map(_.asInstanceOf[SuccessFetchResult].blockId)
    val tmpResultsToAdd: Seq[BlockId] = resultsToAdd.map(_._2.blockId)
    val updateCache = new ArrayBuffer[(BlockId, Byte, Long)]()
    val tmpResultQueue = ArrayBuffer[FetchResult]()
    this.synchronized {
      resultsToAdd.filter(_._1.blockSize > 0).foreach(resultInfo => {
        val notExist = tmpResults.forall(tmpResultsToAdd.contains(_) == false)
        if (notExist) {
          totalBlocksToAdd += 1
          //results.add(resultInfo._2)
          tmpResultQueue += resultInfo._2
          //8.22 SkewTuneAdd
          worker.blocks += ((resultInfo._1.blockId, resultInfo._1))
          updateCache += ((resultInfo._1.blockId, 0x00, resultInfo._1.blockSize))
          //9.10 metrics更新
          resultInfo._1.blockState match {
            case SkewTuneBlockStatus.LOCAL_FETCHED =>
              shuffleMetrics.incLocalBlocksFetched(1)
              shuffleMetrics.incLocalBytesRead(resultInfo._1.blockSize)
            case SkewTuneBlockStatus.REMOTE_FETCHED =>
              shuffleMetrics.incRemoteBlocksFetched(1)
              shuffleMetrics.incRemoteBytesRead(resultInfo._1.blockSize)
          }
        }
      })
      //results.
      val time = System.currentTimeMillis()
      val originSize = results.size()
      tmpResultQueue ++= results
      results.clear()
      results.addAll(tmpResultQueue)
      logInfo(s"addResultTime ${System.currentTimeMillis() - time} ms Origin Size $originSize toAddSize ${tmpResultQueue.size}")

      numBlocksToFetch += totalBlocksToAdd
      if(needLock && isLocked)
        synchronized {
          this.notifyAll()
        }
    }
    //8.23 向Master报告block Status，只在add时需要报告，remove时不需要
    //9.26 bugFix 把Some(worker.taskId) -> fromTaskId
    if(updateCache.nonEmpty)
      worker.reportBlockStatuses(updateCache.map(i=>(i._1,i._2)), Some(fromTaskId),Some(updateCache.map(_._3).sum))
    logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId} 。addFetchResults ： ${updateCache.map(_._1)}")
  }

  //8.21 传入参数改为匹配BLockId，检查block状态必须为fetched(local and remote)
  def removeFetchResults(blocksToRemove: Seq[BlockId]): Seq[(SkewTuneBlockInfo, SuccessFetchResult)] = {
    val toDelete = new ArrayBuffer[(SkewTuneBlockInfo, SuccessFetchResult)]
    val tmpResultBlocksMap = new mutable.HashMap[BlockId, SuccessFetchResult]() ++= results.toArray.filter(_.isInstanceOf[SuccessFetchResult])
      .map(res => {
      val tmp = res.asInstanceOf[SuccessFetchResult]
      (tmp.blockId, tmp)
    })
    this.synchronized {
      blocksToRemove.foreach(blockId => {
        if (tmpResultBlocksMap.contains(blockId) && worker.blocks.contains(blockId)
          && (worker.blocks(blockId).blockState == SkewTuneBlockStatus.LOCAL_FETCHED
          || worker.blocks(blockId).blockState == SkewTuneBlockStatus.REMOTE_FETCHED)) {
          val fetchResult = tmpResultBlocksMap(blockId)
          //8.22 SkewTuneAdd
          val skewTuneBlockInfo = worker.blocks(blockId)
          results.remove(fetchResult)
          toDelete += ((skewTuneBlockInfo, fetchResult))
          //9.10 metrics更新
          skewTuneBlockInfo.blockState match {
            case SkewTuneBlockStatus.LOCAL_FETCHED =>
              shuffleMetrics.decLocalBlocksFetched(1)
              shuffleMetrics.decLocalBytesRead(skewTuneBlockInfo.blockSize)
            case SkewTuneBlockStatus.REMOTE_FETCHED =>
              shuffleMetrics.decRemoteBlocksFetched(1)
              shuffleMetrics.decRemoteBytesRead(skewTuneBlockInfo.blockSize)
          }
        }
      })
      numBlocksToFetch -= toDelete.length
    }
    //logInfo(s"ShuffleBlockFetchIterator on Executor ${blockManager.blockManagerId.executorId}。removeFetchResults ：${toDelete.filter(_._1.blockSize > 0)}")
    toDelete.filter(_._1.blockSize > 0)
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    isZombie = true
    // Release the current buffer if necessary
    currentResult match {
      case SuccessFetchResult(_, _, buf) => buf.release()
      case _ =>
    }

    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, buf) => buf.release()
        case _ =>
      }
    }
  }

  //8.17 sendRequest时包含多个block，而SuccessFetchResult则只包含一个block的buffer
  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val blockIds = req.blocks.map(_._1.toString)

    val address = req.address

    //9.1 SkewTuneAdd : 记录该task到特定blockManager的下载网速
    val downloadStartTime = System.currentTimeMillis

    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          // Only add the buffer to results queue if the iterator is not zombie,
          // i.e. cleanup() has not been called yet.
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            results.put(new SuccessFetchResult(BlockId(blockId), sizeMap(blockId), buf))

            if (worker.blocks.contains(BlockId(blockId))) {
              //9.1 SkewTuneAdd 记录executor到executor的下载网速
              val downloadTimeCost = System.currentTimeMillis() - downloadStartTime
              worker.reportBlockDownloadSpeed(worker.blocks(BlockId(blockId)).blockManagerId.executorId,
                worker.blocks(BlockId(blockId)).blockSize.toFloat / downloadTimeCost)
              //8.21 SkewTuneAdd 远程BLocks得到结果后
              val skewTuneBlockInfo = worker.blocks(BlockId(blockId))
              skewTuneBlockInfo.blockState = SkewTuneBlockStatus.REMOTE_FETCHED
              /*skewTuneBlockInfo.inWhichFetch = None
              skewTuneBlockInfo.inWhichResult = Some(results.peek().asInstanceOf[SuccessFetchResult])*/
              //8.23 向Master报告block已被fetched
              if (isTaskRegistered)
                worker.reportBlockStatuses(Seq((skewTuneBlockInfo.blockId, SkewTuneBlockStatus.REMOTE_FETCHED)))
              else
                reportToMasterCache += ((skewTuneBlockInfo.blockId, SkewTuneBlockStatus.REMOTE_FETCHED))
            }

            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          results.put(new FailureFetchResult(BlockId(blockId), e))
        }
      }
    )
    //8.21 SkewTuneAdd 远程BLocks 放入等待发出Fetch请求后
    req.blocks.foreach(block => worker.blocks(block._1).blockState = SkewTuneBlockStatus.REMOTE_FETCHING)
    //8.23 向Master报告block status 更新
    val tmp: Seq[(BlockId, Byte)] = req.blocks.map(block => (block._1, SkewTuneBlockStatus.REMOTE_FETCHING))
    if (isTaskRegistered)
      worker.reportBlockStatuses(tmp)
    else
      reportToMasterCache ++= tmp
  }

  //8.7 通过address.executorId == blockManager.blockManagerId.executorId判断是否是Local/Remote Block，
  //8.17 分割出来的多个localBlock直接置入localBlocks，增加numBlocksToFetch。
  // 分割出来的多个remoteBlock直接置入remoteBlocks，增加numBlocksToFetch，如果累积的要请求的blocks的size大于maxBytesInFlight/5，
  // 则构造FetchRequest置入remoteRequests，否则只在最后构造FetchRequest
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * [[ManagedBuffer]]'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        //8.31 原生为0，为什么？因为在next()中bytesInFlight要减去这个size，而bytesInFlight是为remote Block Fetch设计的
        //8.31 SkewTuneAdd : 代替方案，Result中的block size使用buf.size获得
        results.put(new SuccessFetchResult(blockId, 0, buf))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)

    //8.21 SkewTuneAdd 远程BLocks 放入等待发出Fetch请求后
    worker.blocks ++= fetchRequests.flatMap(request => {
      request.blocks.map(block => {
        (block._1, SkewTuneBlockInfo(block._1, block._2,
          request.address, SkewTuneBlockStatus.REMOTE_FETCH_WAITING /*,
          Some(request), None)*/))
      })
    }).filter(_._2.blockSize > 0)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    //8.7 在next()时也会执行这一段代码
    //8.23 必须在向skewTune master注册task后才能更新block status，所以之前的更新请求被缓存起来
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    //8.7 直接向本机的BlockManager获得block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))

    synchronized{
      //8.21 SkewTuneAdd 本地BLocks Fetch后
      worker.blocks ++= results.toArray.filter(_.isInstanceOf[SuccessFetchResult]).map(result => {
        val tmp = result.asInstanceOf[SuccessFetchResult]
        (tmp.blockId, SkewTuneBlockInfo(tmp.blockId, tmp.buf.size,
          blockManager.blockManagerId, SkewTuneBlockStatus.LOCAL_FETCHED /*,
        None, Some(tmp)*/))
      }).filter(_._2.blockSize > 0)
      //9.25 SkewTuneAdd Another Iterator
      if(worker.fetchIterator != null){
        worker.fetchIndex += 1
        logInfo(s"task ${worker.taskId} .Another fetchIterator ${worker.fetchIndex} this $this")
        worker.reportNextFetchIterator()
        //worker.allIteratorWorkTime += this.totalTime
      }
      worker.fetchIterator = this
      //9.26
      if(executorInstance.unlockCommandCache.contains(worker.taskId)){
        logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId}. found Unlock Command Cached")
        executorInstance.unlockCommandCache -= worker.taskId
        this.needLock = false
        if(this.isLocked)
          synchronized{
            this.notifyAll()
          }
      }
      //8.23 SkewTuneAdd 向Master报告TaskStart，把task所属的blocks注册上去
      if(!isTaskRegistered)
        worker.registerNewTask(worker.blocks.values.toArray[SkewTuneBlockInfo]) //8.30 Error ERROR ErrorMonitor: Transient association error,HaspMap.values.toSeq:Stream不能序列化
      isTaskRegistered = true
    }
  }

  override def hasNext: Boolean = {
    //9.18 SKewTuneLock
    var r = numBlocksProcessed < numBlocksToFetch
    //isLocked = if(taskLockStatus(taskId) == isLocked) isLocked else taskLockStatus(taskId)
    logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId} " +
      s"numBlocksProcessed: $numBlocksProcessed numBlocksToFetch: $numBlocksToFetch needLock $needLock isLocked $isLocked")
    //9.25 SkewTuneAdd 增加了isLocked状态，防止重复wait()
    if (!r && needLock && !isLocked) {
      synchronized {
        logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId}. wait because Locked")
        isLocked = true
        //9.25 ??? task11/0，10-10时触发了hasNext两次，然后ExecutorBackend的receive()不能进入unlockTask分支，但Akka收到了消息，
        // 为什么会触发两次，为什么会进不了分支？
        this.wait()
      }
      logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId}. resume from Locked")
      r = numBlocksProcessed < numBlocksToFetch
    }
    r
  }

  //8.5 怎么会返回Try[Iterator[Any]]? 【知识点：Scala函数式的异常处理类Try,Success,Failure，及其map/flatMap】
  // 因为case SuccessFetchResult ：
  //        Try(buf.createInputStream()).map{ CompletionIterator[Any, Iterator[Any]] }
  // 为什么是Iterator[Any]?
  // 因为val iter = serializerInstance.deserializeStream(is).asKeyValueIterator
  // 一个block文件中包含多个key-value对，用迭代器包装成Iterator[Any]，调用next()返回的某个block文件的iterator
  override def next(): (BlockId, Try[Iterator[Any]]) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()
    currentResult = results.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    result match {
      case SuccessFetchResult(_, size, _) => bytesInFlight -= size
      case _ =>
    }
    // Send fetch requests up to maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }

    //8.23 SkewTuneAdd 发出缓存的blockstatus更新请求
    if (isTaskRegistered && reportToMasterCache.nonEmpty) {
      worker.reportBlockStatuses(reportToMasterCache)
      reportToMasterCache.clear()
    }
    val iteratorTry: Try[Iterator[Any]] = result match {
      case FailureFetchResult(_, e) =>
        Failure(e)
      //8.7 ???? size这个参数有什么用，只和maxBytesInFlight有关?
      // 每个shuffleBlock都包含下一步待处理的所有partition的record，难道把所有内容都拉下来?
      // 但是一个ShuffleMapTask只处理一个partition的record?
      case SuccessFetchResult(blockId, _, buf) =>
        //8.31 spark自己的代码会不会取得size为0的result？
        bytesActualProcess += buf.size()
        //8.21 SkewTuneAdd BLocks 放入等待发出Fetch请求后
        if (worker.blocks.contains(blockId)) {
          val blockInfo = worker.blocks(blockId)
          blockInfo.blockState = SkewTuneBlockStatus.USED
          logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId} .call next() with block $blockId")
          /*blockInfo.inWhichFetch = None
          blockInfo.inWhichResult = Option(result.asInstanceOf[SuccessFetchResult])*/
          //[@deprecated] 8.23 向Master报告block已被使用(当size大于0时)，如果是最后一个block result了，报告task已完成
          //9.14 SkewTuneAdd 将reportTaskFinish移动到Executor，以便task能结束的更晚点
          if (numBlocksProcessed < numBlocksToFetch) {
            if (blockInfo.blockSize > 0) {
              worker.reportBlockStatuses(Seq((blockInfo.blockId, SkewTuneBlockStatus.USED)))
              blockNumber += 1
            }
          } else {
            logInfo(s"task ${worker.taskId}/${worker.fetchIndex}} on Executor ${blockManager.blockManagerId.executorId}。\n" +
              s"Task Finish with BlockNumber $blockNumber and Size ${shuffleMetrics.localBytesRead} processed.\n" +
              s"TaskShuffleReadMetrics: local ${shuffleMetrics.localBytesRead}")
          }
        }
        // There is a chance that createInputStream can fail (e.g. fetching a local file that does
        // not exist, SPARK-4085). In that case, we should propagate the right exception so
        // the scheduler gets a FetchFailedException.
        Try(buf.createInputStream()).map { is0 =>
          val is = blockManager.wrapForCompression(blockId, is0)
          val iter = serializerInstance.deserializeStream(is).asKeyValueIterator
          CompletionIterator[Any, Iterator[Any]](iter, {
            // Once the iterator is exhausted, release the buffer and set currentResult to null
            // so we don't release it again in cleanup.
            currentResult = null
            buf.release()
          })
        }
    }
    //logInfo(s"on Executor ${blockManager.blockManagerId.executorId} Now to process Result with block ${currentResult.blockId}")

    (result.blockId, iteratorTry)
  }

}


private[spark]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, var blocks: Seq[(BlockId, Long)]) {
    //8.22 SkewTuneAdd
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[spark] sealed trait FetchResult {
    val blockId: BlockId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf [[ManagedBuffer]] for the content.
   */
  private[spark] case class SuccessFetchResult(blockId: BlockId, size: Long, buf: ManagedBuffer)
    extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param e the failure exception
   */
  private[spark] case class FailureFetchResult(blockId: BlockId, e: Throwable)
    extends FetchResult

}
