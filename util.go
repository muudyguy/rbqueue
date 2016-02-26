package queue

/**
It takes first item of a slice and adds it to the end
 */
func takeFirstItemToLast(slice []string) []string {
	firstItem := slice[0]
	slice = append(slice, firstItem)
	slice = slice[1:len(slice)]
	return slice
}

