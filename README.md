# rbqueue
Round Robin style multiple channel queue 

rbqueue is a queue that enlists items according to their group name, and serve them back with round robin scheduling.
It also has a priority system which multiples quantum number according to the priority a group has. For example :

group A  /  priority : 1
group B  /  priority : 1

Both group A and B is served equal times at every cycle. 

group A  /  priority : 1
group B  /  priority : 2

If group A is served 1 time, B is served 2 times.

So it is basically a proportionally linear priority.
