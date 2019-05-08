package com.qubole.sparklens.helper

import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.timespan.JobTimeSpan

import scala.collection.mutable.ListBuffer

object JobOverlapHelper {

  /**
    * Jobs with same sql.execution.id are run in parallel. For completion estimator to work
    * correctly, we need to treat these group of jobs as a single entity.
    * @param ac
    * @return
    */
  def makeJobLists(ac: AppContext): List[List[JobTimeSpan]] = {
    //lets first find the groups by ExecID
    val jobGroupListByExecID = ac.jobSQLExecIdMap.groupBy(x => x._2).map(x => x._2.map(x => ac.jobMap(x._1)).toList).toList

    val allJobIDs = ac.jobMap.map(x => x._1).toSet
    val groupJobIDs = jobGroupListByExecID.flatMap(x => x.map(x => x.jobID)).toSet
    //these jobs are not part of any ExecID
    val leftJobIDs = allJobIDs.diff(groupJobIDs)

    //Merging the two together
    val mergedJobGroupList = new ListBuffer[List[JobTimeSpan]]
    for (elem <- jobGroupListByExecID) {
      mergedJobGroupList.append(elem)
    }

    for (elem <- leftJobIDs) {
      val jobTimeSpan = ac.jobMap(elem)
      val singleItemList = new ListBuffer[JobTimeSpan]
      singleItemList.append(jobTimeSpan)
      mergedJobGroupList.append(singleItemList.toList)
    }
    //we have noticed that jobs withing the same ExecID also have some
    //dependency structure. Not all of the jobs run in parallel. We will
    //try to find isolated/sequential groups here and de-group them.
    val finalJobGroupList = new ListBuffer[List[JobTimeSpan]]
    for (elem <- mergedJobGroupList) {
      if (elem.size == 1) {
        finalJobGroupList.append(elem)
      } else {
        val newLists = splitListIfAppropriate(elem)
        finalJobGroupList.appendAll(newLists)
      }
    }
    finalJobGroupList.toList
  }

  def estimatedTimeSpentInJobs(ac: AppContext): Long = {
    val jobGroupsList = makeJobLists(ac)
    jobGroupsList.map(x => (x.map(x => x.endTime).max - x.map(x => x.startTime).min)).sum
  }

  def criticalPathForAllJobs(ac: AppContext):Long = {
    makeJobLists(ac).map( group => group.map(job => job.computeCriticalTimeForJob()).max).sum
  }

  private def splitListIfAppropriate(list: List[JobTimeSpan]): List[List[JobTimeSpan]] = {
    val newList = new ListBuffer[List[JobTimeSpan]]

    var tempList = new ListBuffer[JobTimeSpan]
    for (job <- list.sortWith((a, b) => a.startTime < b.startTime)) {
      if (tempList.isEmpty) {
        tempList.append(job)
      } else {
        if (tempList.last.endTime < job.startTime) {
          //serial
          newList.append(tempList.toList)
          tempList = new ListBuffer[JobTimeSpan]
          tempList.append(job)
        } else {
          //may be parallel
          tempList.append(job)
        }
      }
    }
    if (!tempList.isEmpty) {
      newList.append(tempList.toList)
    }
    newList.toList
  }
}
