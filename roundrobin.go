package queue
import (
	"sync"
	"fmt"
)


type RoundRobinQueue struct {
	Quantum                     int
	groupMessageBoxMap          map[string][]interface{}
	priorityMap                 map[string]int
	rotatingGroupQueue          []string
	currentQuantumState         int
	currentlyProcessedGroupName string

	totalItemCount              int

	enlistMutex                 sync.Mutex
	getOneMutex                 sync.Mutex
	setGroupMutex               sync.Mutex
	priorityMapMutex			sync.Mutex
}

//todo disgusting method, change
func (selfPtr *RoundRobinQueue) init() {

	if selfPtr.currentlyProcessedGroupName == "" {
		if len(selfPtr.rotatingGroupQueue) > 0 {
			selfPtr.currentlyProcessedGroupName = selfPtr.rotatingGroupQueue[0]
		}
	}

	if selfPtr.groupMessageBoxMap == nil {
		selfPtr.groupMessageBoxMap = make(map[string][]interface{})
	}

	if selfPtr.priorityMap == nil {
		selfPtr.priorityMap = make(map[string]int)
	}
}

/**
Add an item into the queue
 */
func (selfPtr *RoundRobinQueue) Enlist(group string, item interface{}) {
	selfPtr.enlistMutex.Lock()
	defer selfPtr.enlistMutex.Unlock()

	selfPtr.priorityMapMutex.Lock()
	if len(selfPtr.priorityMap == 0) {
		panic(fmt.Errorf("There are no priorities set within the map !"))
	}
	selfPtr.priorityMapMutex.Unlock()

	selfPtr.init()

	groupSlice := selfPtr.groupMessageBoxMap[group]
	groupSlice = append(groupSlice, item)
	selfPtr.groupMessageBoxMap[group] = groupSlice

	//increase total item count
	selfPtr.totalItemCount = selfPtr.totalItemCount + 1
}

/**
Rotate the priority slice for the next type of message box process
 */
func (selfPtr *RoundRobinQueue) rotatePrioritySlice() {
	//Set up the priority slice for the next get
	selfPtr.rotatingGroupQueue = takeFirstItemToLast(selfPtr.rotatingGroupQueue)
}

/**
Pass the group and start processing the next one
 */
func (selfPtr *RoundRobinQueue) passGroup() bool {
	previousCurrent := selfPtr.currentlyProcessedGroupName

	selfPtr.rotatePrioritySlice()

	//Reset quantum state
	selfPtr.currentQuantumState = 0

	//Set current group to the next one in the queue
	selfPtr.currentlyProcessedGroupName = selfPtr.rotatingGroupQueue[0]

	if previousCurrent == selfPtr.currentlyProcessedGroupName {
		return false
	}
	return true
}

/**
Low performance iteration
Should track availability implicitly
 */
func (selfPtr *RoundRobinQueue) CheckAvailableMessage() (int) {
	var count int = 0
	for k, _ := range selfPtr.groupMessageBoxMap {
		groupMessageBox := selfPtr.groupMessageBoxMap[k]
		if len(groupMessageBox) > 0 {
			count += 1
		}
	}
	return count

}


func (selfPtr *RoundRobinQueue) resolveNextItemAndReturn() (interface{}, bool) {
	//If there are no items, just return nil
	if selfPtr.totalItemCount == 0 {
		return nil, false
	}


	var itemToReturn interface{}
	var itemFound bool

	//get message box of the current group
	groupMessageBox := selfPtr.groupMessageBoxMap[selfPtr.currentlyProcessedGroupName]

	//if group message box has some message in it, process it
	if len(groupMessageBox) > 0 {
		//todo make this snippet into a method
		itemToReturn = groupMessageBox[0]
		itemFound = true

		//Remove the first item of the group's message box, as we already took it into itemToReturn
		groupMessageBox = groupMessageBox[1:len(groupMessageBox)]
		selfPtr.groupMessageBoxMap[selfPtr.currentlyProcessedGroupName] = groupMessageBox

		//increase the quantum state
		selfPtr.currentQuantumState += 1
	} else {
		//There are no messages in the box, so pass the group for the next one, which will also reset the quantum state
		pass := selfPtr.passGroup()
		if pass {
			//pass was succesful, there are more than two groups
			return selfPtr.resolveNextItemAndReturn()
		} else {
			//There is only just one group. this block is now irrelevant, because if there are 0 total counts we just return nil
			//Keep it here as it makes it easier to understand the flow
			return nil, false
		}
	}

	//Quantum for the group is at the limit, pass the group before returning the last message in its box
	if selfPtr.currentQuantumState == selfPtr.Quantum * selfPtr.priorityMap[selfPtr.currentlyProcessedGroupName] {
		selfPtr.passGroup()
	}

	return itemToReturn, itemFound
}

/**
Get one message from the current group's message box
 */
func (selfPtr *RoundRobinQueue) GetOne() (interface{}, bool) {
	selfPtr.getOneMutex.Lock()
	defer selfPtr.getOneMutex.Unlock()

	selfPtr.priorityMapMutex.Lock()
	if len(selfPtr.priorityMap == 0) {
		panic(fmt.Errorf("There are no priorities set within the map !"))
	}
	selfPtr.priorityMapMutex.Unlock()

	selfPtr.init()

	if selfPtr.totalItemCount == 0 {
		return nil, false
	} else {
		selfPtr.totalItemCount = selfPtr.totalItemCount - 1
		return selfPtr.resolveNextItemAndReturn()
	}
}

/**
Set a new group and its priority
 */
func (selfPtr *RoundRobinQueue) SetGroup(name string, priority int) {
	selfPtr.setGroupMutex.Lock()
	defer selfPtr.setGroupMutex.Unlock()

	selfPtr.init()

	selfPtr.priorityMap[name] = priority
	selfPtr.rotatingGroupQueue = append(selfPtr.rotatingGroupQueue, name)
}

/**
Get total item count of the queue
 */
func (selfPtr *RoundRobinQueue) GetTotalItemCount() int {
	return selfPtr.totalItemCount
}