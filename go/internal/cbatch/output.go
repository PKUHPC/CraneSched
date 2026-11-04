package cbatch

import (
	"fmt"
	"strconv"
	"strings"
)

// formatJobSubmissionOutput keeps cbatch's native output separate from the
// output contract exposed by the command wrappers.
func formatJobSubmissionOutput(jobIDs []uint32) string {
	if len(jobIDs) == 0 {
		return ""
	}
	lines := make([]string, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		if FlagParsable {
			lines = append(lines, strconv.FormatUint(uint64(jobID), 10))
		} else {
			lines = append(lines, fmt.Sprintf("Submitted batch job %d", jobID))
		}
	}
	return strings.Join(lines, "\n") + "\n"
}
