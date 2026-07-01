package gokafka

import "testing"

func aggDataItem(topic string, part int32, off int64) fetchItem {
	rec := Record{Topic: topic, Partition: part, Offset: off, Value: []byte("v")}
	return fetchItem{topic: topic, partition: part, offset: off, rec: &rec}
}

func aggMarkerItem(topic string, part int32, off int64) fetchItem {
	return fetchItem{topic: topic, partition: part, offset: off} // rec == nil
}

// The multi-broker over-fetch loss: two brokers each return maxPoll records in
// parallel; only the first broker's records fit the poll. The second broker's
// partition must NOT have its cursor advanced — its already-fetched records are
// dropped from this poll and MUST be re-fetched next poll, never skipped. (The
// old code bumped their offsets at decode time and then dropped them via
// out[:maxPoll], silently losing them.)
func TestAggregateFetchesNoOverFetchLoss(t *testing.T) {
	node0 := []fetchItem{aggDataItem("t", 0, 0), aggDataItem("t", 0, 1), aggDataItem("t", 0, 2)}
	node1 := []fetchItem{aggDataItem("t", 1, 0), aggDataItem("t", 1, 1), aggDataItem("t", 1, 2)}

	out, cursor := aggregateFetches([][]fetchItem{node0, node1}, 3)

	if len(out) != 3 {
		t.Fatalf("delivered %d records, want 3 (maxPoll)", len(out))
	}
	for _, r := range out {
		if r.Partition != 0 {
			t.Fatalf("delivered a partition-%d record; only partition 0 fit maxPoll", r.Partition)
		}
	}
	if got := cursor[partKey{"t", 0}]; got != 3 {
		t.Fatalf("partition 0 cursor = %d, want 3", got)
	}
	if _, ok := cursor[partKey{"t", 1}]; ok {
		t.Fatal("partition 1 cursor must be UNSET — its fetched records were dropped by the maxPoll cut and must be re-fetched, not skipped (data loss)")
	}
}

// A partition cut mid-run advances only to the last delivered record; the
// dropped tail is re-fetched.
func TestAggregateFetchesMidRunCut(t *testing.T) {
	items := []fetchItem{aggDataItem("t", 0, 0), aggDataItem("t", 0, 1), aggDataItem("t", 0, 2), aggDataItem("t", 0, 3)}
	out, cursor := aggregateFetches([][]fetchItem{items}, 2)
	if len(out) != 2 {
		t.Fatalf("delivered %d, want 2", len(out))
	}
	if got := cursor[partKey{"t", 0}]; got != 2 {
		t.Fatalf("cursor = %d, want 2 (offsets 2 and 3 must be re-fetched)", got)
	}
}

// Markers advance the cursor without counting toward maxPoll, so a
// read_committed consumer never stalls re-fetching a marker — but a marker is
// only applied when it survives the cut.
func TestAggregateFetchesMarkersAdvanceWithoutStall(t *testing.T) {
	// Trailing markers under maxPoll: cursor advances past them.
	withMarkers := []fetchItem{aggDataItem("t", 0, 0), aggDataItem("t", 0, 1), aggMarkerItem("t", 0, 2), aggMarkerItem("t", 0, 3)}
	out, cursor := aggregateFetches([][]fetchItem{withMarkers}, 10)
	if len(out) != 2 {
		t.Fatalf("delivered %d data records, want 2 (markers are not records)", len(out))
	}
	if got := cursor[partKey{"t", 0}]; got != 4 {
		t.Fatalf("cursor = %d, want 4 (past the trailing markers)", got)
	}

	// A partition returning ONLY markers (aborted transaction) must still advance
	// past them, or the consumer re-fetches them forever.
	onlyMarkers := []fetchItem{aggMarkerItem("t", 1, 0), aggMarkerItem("t", 1, 1), aggMarkerItem("t", 1, 2)}
	out2, cursor2 := aggregateFetches([][]fetchItem{onlyMarkers}, 10)
	if len(out2) != 0 {
		t.Fatalf("delivered %d markers as records, want 0", len(out2))
	}
	if got := cursor2[partKey{"t", 1}]; got != 3 {
		t.Fatalf("all-marker cursor = %d, want 3 (must advance to avoid a re-fetch stall)", got)
	}
}

// Across two consecutive polls, every record is delivered exactly once: the
// records dropped by the first poll's cut are picked up by the second, and none
// are skipped or duplicated. This is the end-to-end no-loss guarantee.
func TestAggregateFetchesUnionLosesNothing(t *testing.T) {
	// Two partitions with 4 records each, fetched from two brokers, maxPoll 3.
	seen := map[partKey][]int64{}
	// Simulate live cursors starting at 0 for both partitions.
	pos := map[partKey]int64{{"t", 0}: 0, {"t", 1}: 0}

	for poll := 0; poll < 5; poll++ {
		var n0, n1 []fetchItem
		for off := pos[partKey{"t", 0}]; off < 4; off++ {
			n0 = append(n0, aggDataItem("t", 0, off))
		}
		for off := pos[partKey{"t", 1}]; off < 4; off++ {
			n1 = append(n1, aggDataItem("t", 1, off))
		}
		out, cursor := aggregateFetches([][]fetchItem{n0, n1}, 3)
		for _, r := range out {
			seen[partKey{r.Topic, r.Partition}] = append(seen[partKey{r.Topic, r.Partition}], r.Offset)
		}
		// Apply the cursor advances (only for partitions that appeared).
		for pk, next := range cursor {
			pos[pk] = next
		}
	}

	for _, p := range []int32{0, 1} {
		got := seen[partKey{"t", p}]
		if len(got) != 4 {
			t.Fatalf("partition %d delivered %v, want 4 distinct records", p, got)
		}
		for i, off := range got {
			if off != int64(i) {
				t.Fatalf("partition %d delivered offset %d at position %d — reorder/duplication/loss", p, off, i)
			}
		}
	}
}
