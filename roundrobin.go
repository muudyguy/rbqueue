package queue
import (
	"sync"
	"fmt"
)

//todo Reduce locks with atomic ops?
type RoundRobinQueue struct {
	Quantum                     int
	groupMessageBoxMap          map[string][]interface{}
	defaultBox				    []interface{}
	priorityMap                 map[string]int
	rotatingGroupQueue          []string
	currentQuantumState         int
	currentlyProcessedGroupName string

	totalItemCount              *int

	globalLock                  *sync.Mutex

}

func NewRoundRobinQueue() *RoundRobinQueue {
	rrq := RoundRobinQueue{}
	if len(rrq.rotatingGroupQueue) > 0 {
		rrq.currentlyProcessedGroupName = rrq.rotatingGroupQueue[0]
	}

	rrq.groupMessageBoxMap = make(map[string][]interface{})

	rrq.groupMessageBoxMap["rrqabsolute"] = make([]interface{}, 0)

	rrq.priorityMap = make(map[string]int)

	rrq.globalLock = new(sync.Mutex)

	rrq.defaultBox = make([]interface{}, 0)

	rrq.totalItemCount = new(int)

	return &rrq
}

func (selfPtr *RoundRobinQueue) checkIfGroupExists(group string) {
	_, found := selfPtr.priorityMap[group]


	if !found {
		panic(fmt.Errorf("This group does not exist in the queue priority map !"))
	}

}

/**
Add an item into the queue
 */
func (selfPtr *RoundRobinQueue) Enlist(group string, item interface{}) {
	selfPtr.globalLock.Lock()
	defer selfPtr.globalLock.Unlock()

	if len(selfPtr.priorityMap) == 0 {
		selfPtr.defaultBox = append(selfPtr.defaultBox, item)
	} else {
		selfPtr.checkIfGroupExists(group)

		groupMessageBox := selfPtr.groupMessageBoxMap[group]
		groupMessageBox = append(groupMessageBox, item)
		//	fmt.Println("Now the length of message box with group name " + group + "  is : " + strconv.Itoa(len(groupMessageBox)))
		//	fmt.Println(item)
		selfPtr.groupMessageBoxMap[group] = groupMessageBox
	}

	//increase total item count
	*selfPtr.totalItemCount += 1

}

func (selfPtr *RoundRobinQueue) EnlistAbsolutePriority(item interface{}) {
	selfPtr.globalLock.Lock()
	defer selfPtr.globalLock.Unlock()

	fmt.Println("I AM INCREASING")

	//todo This is very low performance obviously, find a way to fix overcopying
	if len(selfPtr.priorityMap) == 0 {
		selfPtr.defaultBox = append([]interface{}{item}, selfPtr.defaultBox...)
		fmt.Println(selfPtr.defaultBox)
	} else {
		selfPtr.groupMessageBoxMap["rrqabsolute"] = append(selfPtr.groupMessageBoxMap["rrqabsolute"], item)
	}

	//increase total item count
	*selfPtr.totalItemCount += 1
}

/**
Rotate the priority slice for the next type of message box process
 */
func (selfPtr *RoundRobinQueue) rotateQueue() {
	//todo DO I NEED LOCKING HERE?
	//Set up the priority slice for the next get
	selfPtr.rotatingGroupQueue = takeFirstItemToLast(selfPtr.rotatingGroupQueue)
}

/**
Pass the group and start processing the next one
 */
func (selfPtr *RoundRobinQueue) passGroup() bool {
	selfPtr.rotateQueue()

	//Reset quantum state
	selfPtr.currentQuantumState = 0


	previousCurrent := selfPtr.currentlyProcessedGroupName
	//Set current group to the next one in the queue
	selfPtr.currentlyProcessedGroupName = selfPtr.rotatingGroupQueue[0]

	if previousCurrent == selfPtr.currentlyProcessedGroupName {
		return false
	}
	return true

}


func (selfPtr *RoundRobinQueue) resolveNextItemAndReturn() (interface{}, bool) {
	//If there are no items, just return nil
	if *selfPtr.totalItemCount == 0 {
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
	selfPtr.globalLock.Lock()
	defer selfPtr.globalLock.Unlock()

	if *selfPtr.totalItemCount == 0 {
		fmt.Println("HERE")
		return nil, false
	}



	//If there is no priority set
	if len(selfPtr.priorityMap) == 0 {
		if len(selfPtr.defaultBox) > 0 {
			item := selfPtr.defaultBox[0]
			//todo Would crash if default box has only 1 item??
			selfPtr.defaultBox = selfPtr.defaultBox[1:len(selfPtr.defaultBox)]
			return item, true
		} else {
			fmt.Println("HERE Y")
			return nil, false
		}
	} else {
		//todo Checking this everytime is very ugly
		absolutePriorityBox := selfPtr.groupMessageBoxMap["rrqabsolute"]
		if len(absolutePriorityBox) > 0 {
			item := absolutePriorityBox[len(absolutePriorityBox) - 1]
			*selfPtr.totalItemCount -= 1
			return item, true
		}
	}

	item, found := selfPtr.resolveNextItemAndReturn()
	*selfPtr.totalItemCount -= 1
	return item, found

}

/**
Set a new group and its priority
 */
func (selfPtr *RoundRobinQueue) SetGroup(name string, priority int) {
	selfPtr.globalLock.Lock()
	defer selfPtr.globalLock.Unlock()

	selfPtr.priorityMap[name] = priority
	selfPtr.rotatingGroupQueue = append(selfPtr.rotatingGroupQueue, name)
}

/**
Get total item count of the queue
 */
func (selfPtr *RoundRobinQueue) GetTotalItemCount() int {
	return *selfPtr.totalItemCount
}