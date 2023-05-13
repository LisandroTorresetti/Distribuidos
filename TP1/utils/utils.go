package utils

import "regexp"

func ContainsString(targetString string, sliceOfStrings []string) bool {
	for i := range sliceOfStrings {
		if sliceOfStrings[i] == targetString {
			return true
		}
	}
	return false
}

func GetRandomID() int {
	/*// initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// generate a random number between 1 and 3
	return rand.Intn(3) + 1*/

	return 1 // FIXME: use implementation above
}

// GetQuarter returns the quarter to which belongs the data based on the date
func GetQuarter(month int) string {
	/*if month < 4 {
		return "Q1"
	}

	if 4 <= month && month < 7 {
		return "Q2"
	}

	if 7 <= month && month < 10 {
		return "Q3"
	}
	return "Q4"*/
	return "Q1" // FIXME: use implementation above
}

// GetTargetStage returns the target stage from topic's name. A topic name has the following structure: actualStage-targetStage-topic
func GetTargetStage(topicName string) string {
	regex := regexp.MustCompile(`^[^-]+-([^-]+)-topic$`)
	matches := regex.FindStringSubmatch(topicName)
	return matches[1]
}
