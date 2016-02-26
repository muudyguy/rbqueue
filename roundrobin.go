package queue
import (
	"sync"
	"fmt"
)



type RoundRobinQueue struct {
	Quantum                     int
	groupMap                    map[string][]interface{}
	priorityMap                 map[string]int
	rotatingGroupQueue          []string
	currentQuantumState         int
	currentlyProcessedGroupName string

	enlistMutex                 sync.Mutex
	getOneMutex                 sync.Mutex
	setGroupMutex               sync.Mutex

}

func (selfPtr *RoundRobinQueue) init() {
	if selfPtr.currentlyProcessedGroupName == "" {
		if len(selfPtr.rotatingGroupQueue) > 0 {
			selfPtr.currentlyProcessedGroupName = selfPtr.rotatingGroupQueue[0]
		}
	}

	if selfPtr.groupMap == nil {
		selfPtr.groupMap = make(map[string][]interface{})
	}

	if selfPtr.priorityMap == nil {
		selfPtr.priorityMap = make(map[string]int)
	}
}

func (self *RoundRobinQueue) Enlist(group string, item interface{}) {
	self.enlistMutex.Lock()
	defer self.enlistMutex.Unlock()

	self.init()

	groupSlice := self.groupMap[group]
	groupSlice = append(groupSlice, item)
	self.groupMap[group] = groupSlice
}

func takeFirstItemToLast(slice []string) []string {
	firstItem := slice[0]
	slice = append(slice, firstItem)
	slice = slice[1:len(slice)]
	return slice
}

func (selfPtr *RoundRobinQueue) rotatePrioritySlice() {
	//Set up the priority slice for the next get
	selfPtr.rotatingGroupQueue = takeFirstItemToLast(selfPtr.rotatingGroupQueue)
}

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

func (selfPtr *RoundRobinQueue) resolveNextItemAndReturn() (interface{}, bool) {
	var itemToReturn interface{}
	var itemFound bool
	//Get the item from the queue and remove
	groupMessageBox := selfPtr.groupMap[selfPtr.currentlyProcessedGroupName]

	if len(groupMessageBox) > 0 {
		itemToReturn = groupMessageBox[0]
		itemFound = true

		groupMessageBox = groupMessageBox[1:len(groupMessageBox)]
		selfPtr.groupMap[selfPtr.currentlyProcessedGroupName] = groupMessageBox

		selfPtr.currentQuantumState += 1
	} else {
		pass := selfPtr.passGroup()
		if pass {
			return selfPtr.resolveNextItemAndReturn()
		} else {
			return nil, false
		}
	}

	//Go to the next group
	if selfPtr.currentQuantumState == selfPtr.Quantum * selfPtr.priorityMap[selfPtr.currentlyProcessedGroupName] {
		fmt.Println("passing group")
		selfPtr.passGroup()
	} else {
		fmt.Println("not passing group")
	}

	return itemToReturn, itemFound
}

func (self *RoundRobinQueue) GetOne() (interface{}, bool) {
	self.getOneMutex.Lock()
	defer self.getOneMutex.Unlock()

	self.init()
	return self.resolveNextItemAndReturn()
}

func (selfPtr *RoundRobinQueue) SetGroup(name string, priority int) {
	selfPtr.setGroupMutex.Lock()
	defer selfPtr.setGroupMutex.Unlock()

	selfPtr.init()

	selfPtr.priorityMap[name] = priority
	selfPtr.rotatingGroupQueue = append(selfPtr.rotatingGroupQueue, name)


}

