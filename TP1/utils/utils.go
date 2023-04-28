package utils

func ContainsString(targetString string, sliceOfStrings []string) bool {
	for i := range sliceOfStrings {
		if sliceOfStrings[i] == targetString {
			return true
		}
	}
	return false
}
