package health

import "sort"

func sortResults(r []ProbeResult) {
	sort.SliceStable(r, func(i, j int) bool { return r[i].At.Before(r[j].At) })
}
