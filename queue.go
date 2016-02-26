package queue

type GroupQueue interface {
	Enlist(group string, item interface{})
	GetOne() (interface{}, bool)
	Peek() (interface{}, bool)
}
