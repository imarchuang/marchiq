package storage

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func produceN(t *testing.T, b *Broker, topic string, partition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := b.Produce(topic, partition, nil, []byte{byte(i)}, AcksLeader); err != nil {
			t.Fatal(err)
		}
	}
}

func fetchOffsets(t *testing.T, b *Broker, group, topic, member string) []PartitionFetch {
	t.Helper()
	parts, err := b.FetchGroup(group, topic, member, 1000, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

func join(t *testing.T, b *Broker, group, topic, member string) Assignment {
	t.Helper()
	a, err := b.JoinGroup(group, topic, member)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func heartbeat(t *testing.T, b *Broker, group, topic, member string, generation int) HeartbeatResult {
	t.Helper()
	hb, err := b.Heartbeat(group, topic, member, generation)
	if err != nil {
		t.Fatal(err)
	}
	return hb
}

// ageLastSeen backdates a member's last heartbeat so a manual reaper pass
// sees it as expired without any sleeping — the same trick ageAllSegments
// uses for retention mtimes.
func ageLastSeen(t *testing.T, b *Broker, group, topic, member string, age time.Duration) {
	t.Helper()
	b.gmu.Lock()
	defer b.gmu.Unlock()
	gt := b.groups[group].topics[topic]
	gt.LastSeen[member] = time.Now().Add(-age)
}

// Live-member assignment: partition p goes to the member at index
// p % len(members) of the CURRENT live list, so every join redistributes and
// the generation bumps. An idempotent rejoin changes nothing; there is no
// "group full" and no declared size anymore.
func TestJoinGroupLiveAssignment(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	b.SetGroupReapInterval(time.Hour) // deterministic: manual reaps only in this test
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	a := join(t, b, "g1", "events", "a")
	if a.Generation != 1 || !slices.Equal(a.Partitions, []int{0, 1, 2, 3}) {
		t.Fatalf("%+v", a)
	}
	bb := join(t, b, "g1", "events", "b")
	if bb.Generation != 2 || !slices.Equal(bb.Partitions, []int{1, 3}) {
		t.Fatalf("%+v", bb)
	}
	// a's assignment shrank with b's arrival; the heartbeat is how a learns.
	if hb := heartbeat(t, b, "g1", "events", "a", 2); !slices.Equal(hb.Partitions, []int{0, 2}) {
		t.Fatalf("%+v", hb)
	}
	// Idempotent rejoin: same generation, same assignment.
	again := join(t, b, "g1", "events", "a")
	if again.Generation != 2 || !slices.Equal(again.Partitions, []int{0, 2}) {
		t.Fatalf("%+v", again)
	}
	// No "group full": a third member just redistributes again.
	c := join(t, b, "g1", "events", "c")
	if c.Generation != 3 || !slices.Equal(c.Partitions, []int{2}) {
		t.Fatalf("%+v", c)
	}
	if hb := heartbeat(t, b, "g1", "events", "a", 3); !slices.Equal(hb.Partitions, []int{0, 3}) {
		t.Fatalf("%+v", hb)
	}
	// An empty member id gets a generated one that never collides.
	d := join(t, b, "g1", "events", "")
	if d.Member != "member-1" || d.Generation != 4 || !slices.Equal(d.Partitions, []int{3}) {
		t.Fatalf("%+v", d)
	}
	if _, err := b.JoinGroup("g1", "ghost", "a"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.JoinGroup("bad/name", "events", "a"); err == nil {
		t.Fatal("invalid group name must fail")
	}
}

// More members than partitions: trailing members get an empty assignment —
// not an error (Kafka's idle consumer).
func TestJoinMoreMembersThanPartitions(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	join(t, b, "g1", "events", "m1")
	join(t, b, "g1", "events", "m2")
	m3 := join(t, b, "g1", "events", "m3")
	if m3.Generation != 3 || len(m3.Partitions) != 0 {
		t.Fatalf("%+v", m3)
	}
	if m3.Partitions == nil {
		t.Fatal("empty assignment must marshal as [], not null")
	}
	// The idle member fetches its (empty) assignment without error.
	if parts := fetchOffsets(t, b, "g1", "events", "m3"); len(parts) != 0 {
		t.Fatalf("%+v", parts)
	}
}

// The kafka-notes §3 zombie timeline, end to end:
//
//	t0  m1, m2 join (generation bumps to 2); m1 owns [0], m2 owns [1]
//	t1  m1 "GC pauses" — no heartbeats (simulated by backdating LastSeen)
//	t2  the reaper evicts m1, generation bumps to 3, m2 now owns [0,1]
//	t3  m1 wakes and commits with the OLD generation → fenced
//	t4  m2's commit with the current generation succeeds; m1 rejoins as a
//	    NEW member at the end of the order and gets a fresh assignment
func TestGroupZombieFencing(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	b.SetGroupReapInterval(time.Hour) // background reaper off; manual passes only
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 5)
	produceN(t, b, "events", 1, 5)

	// t0
	if a := join(t, b, "g1", "events", "m1"); a.Generation != 1 || !slices.Equal(a.Partitions, []int{0, 1}) {
		t.Fatalf("%+v", a)
	}
	if a := join(t, b, "g1", "events", "m2"); a.Generation != 2 || !slices.Equal(a.Partitions, []int{1}) {
		t.Fatalf("%+v", a)
	}
	if hb := heartbeat(t, b, "g1", "events", "m1", 2); !slices.Equal(hb.Partitions, []int{0}) {
		t.Fatalf("%+v", hb)
	}
	// m2 fetches and commits p1 while healthy (generation 2).
	parts := fetchOffsets(t, b, "g1", "events", "m2")
	if len(parts) != 1 || parts[0].Partition != 1 || len(parts[0].Records) != 5 {
		t.Fatalf("%+v", parts)
	}
	if err := b.CommitOffset("g1", "events", 1, 4, 2); err != nil {
		t.Fatal(err)
	}

	// t1–t2: m1 goes silent past the session timeout; m2 stays alive.
	ageLastSeen(t, b, "g1", "events", "m1", 2*DefaultSessionTimeout)
	evicted, err := b.ReapExpiredMembers(time.Now())
	if err != nil || evicted != 1 {
		t.Fatalf("evicted=%d err=%v", evicted, err)
	}
	hb := heartbeat(t, b, "g1", "events", "m2", 3)
	if hb.Generation != 3 || !slices.Equal(hb.Partitions, []int{0, 1}) {
		t.Fatalf("%+v", hb)
	}
	// The evicted zombie's heartbeat names no member...
	if _, err := b.Heartbeat("g1", "events", "m1", 2); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	// ...and its fetch fails the same way: member resolution runs over the
	// CURRENT live list.
	if _, err := b.FetchGroup("g1", "events", "m1", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}

	// t3: m1 wakes from its "GC pause" and commits with the old generation.
	if err := b.CommitOffset("g1", "events", 0, 4, 2); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("err=%v", err)
	}
	// t4: m2's commit with the current generation lands.
	if err := b.CommitOffset("g1", "events", 0, 4, 3); err != nil {
		t.Fatal(err)
	}

	// m1 rejoins: a NEW join at the end of the order (no static-membership
	// reclaim), the generation bumps, assignments rebalance again.
	a := join(t, b, "g1", "events", "m1")
	if a.Generation != 4 || !slices.Equal(a.Partitions, []int{1}) {
		t.Fatalf("%+v", a)
	}
	if hb := heartbeat(t, b, "g1", "events", "m2", 4); !slices.Equal(hb.Partitions, []int{0}) {
		t.Fatalf("%+v", hb)
	}
	// m1 inherits p1 and resumes where the GROUP committed (offset 4):
	// committed offsets survive membership changes.
	parts = fetchOffsets(t, b, "g1", "events", "m1")
	if len(parts) != 1 || parts[0].Partition != 1 || len(parts[0].Records) != 0 || parts[0].NextOffset != 5 {
		t.Fatalf("%+v", parts)
	}
}

// Heartbeat refreshes the session (the reaper spares the member), unknown
// group/member is not-found, and a stale generation is fenced with the
// current generation in the error. A fenced heartbeat still proves liveness.
func TestGroupHeartbeat(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	b.SetGroupReapInterval(time.Hour)
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	join(t, b, "g1", "events", "m1")
	if _, err := b.Heartbeat("ghost", "events", "m1", 1); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.Heartbeat("g1", "events", "ghost", 1); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.Heartbeat("g1", "ghost", "m1", 1); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	// A second member bumps the generation; m1's stale heartbeat is fenced
	// and the error names the current generation.
	join(t, b, "g1", "events", "m2")
	if _, err := b.Heartbeat("g1", "events", "m1", 1); !errors.Is(err, ErrGenerationFence) ||
		!strings.Contains(err.Error(), "current 2") {
		t.Fatalf("err=%v", err)
	}
	// The fenced heartbeat still refreshed m1's session: age its stamp to
	// near-expiry, heartbeat, then reap past where the OLD stamp would have
	// expired — m1 survives.
	ageLastSeen(t, b, "g1", "events", "m1", DefaultSessionTimeout-time.Second)
	if hb := heartbeat(t, b, "g1", "events", "m1", 2); hb.Generation != 2 || !slices.Equal(hb.Partitions, []int{0}) {
		t.Fatalf("%+v", hb)
	}
	evicted, err := b.ReapExpiredMembers(time.Now().Add(2 * time.Second))
	if err != nil || evicted != 0 {
		t.Fatalf("evicted=%d err=%v", evicted, err)
	}
	// m2 never heartbeated: age it out and it is the one evicted.
	ageLastSeen(t, b, "g1", "events", "m2", 2*DefaultSessionTimeout)
	evicted, err = b.ReapExpiredMembers(time.Now())
	if err != nil || evicted != 1 {
		t.Fatalf("evicted=%d err=%v", evicted, err)
	}
	// m1 is now the sole member and owns everything under generation 3.
	if hb := heartbeat(t, b, "g1", "events", "m1", 3); !slices.Equal(hb.Partitions, []int{0, 1}) {
		t.Fatalf("%+v", hb)
	}
}

// LeaveGroup: the survivors' assignments are recomputed over the shrunken
// live list and the generation bumps; leaving twice or from an unknown group
// is not-found.
func TestLeaveGroupRedistributes(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	b.SetGroupReapInterval(time.Hour)
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	join(t, b, "g1", "events", "m1")
	join(t, b, "g1", "events", "m2") // gen 2: m1 [0,2], m2 [1,3]
	if err := b.LeaveGroup("g1", "events", "m1"); err != nil {
		t.Fatal(err)
	}
	hb := heartbeat(t, b, "g1", "events", "m2", 3)
	if hb.Generation != 3 || !slices.Equal(hb.Partitions, []int{0, 1, 2, 3}) {
		t.Fatalf("%+v", hb)
	}
	if _, err := b.FetchGroup("g1", "events", "m1", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if err := b.LeaveGroup("g1", "events", "m1"); !errors.Is(err, ErrGroupNotFound) { // already gone
		t.Fatalf("err=%v", err)
	}
	if err := b.LeaveGroup("ghost", "events", "m2"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
}

// The last member leaving keeps the group and its committed offsets: the
// retention clamp (min committed+1 over joined groups) must still hold, and
// a later join resumes from the committed cursor.
func TestLeaveLastMemberKeepsOffsets(t *testing.T) {
	b, _ := newRetentionBroker(t, TopicConfig{Name: "events", Partitions: 1, RetentionMS: 60_000}, 20)
	b.SetGroupReapInterval(time.Hour)
	// segments: [0..3] [4..7] [8..11] [12..15] [16..19 active]
	a := join(t, b, "g1", "events", "m1")
	if err := b.CommitOffset("g1", "events", 0, 9, a.Generation); err != nil { // clamp B = 10
		t.Fatal(err)
	}
	if err := b.LeaveGroup("g1", "events", "m1"); err != nil {
		t.Fatal(err)
	}
	// The group still exists with zero members; the generation moved on.
	lags, err := b.GroupLag("g1")
	if err != nil || len(lags) != 1 || len(lags[0].Members) != 0 || lags[0].Generation != a.Generation+1 {
		t.Fatalf("%+v %v", lags, err)
	}
	ageAllSegments(t, b, "events", 0, 2*time.Minute)
	deleted, err := b.EnforceRetention()
	if err != nil || deleted != 2 { // the clamp holds: [8..11] contains committed+1
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if off, err := b.GetOffsets("events", 0); err != nil || off.Earliest != 8 {
		t.Fatalf("%+v %v", off, err)
	}
	// A new member joins (generation bumps) and resumes at committed+1 = 10.
	join(t, b, "g1", "events", "m2")
	parts := fetchOffsets(t, b, "g1", "events", "m2")
	if len(parts) != 1 || len(parts[0].Records) != 10 || parts[0].Records[0].Offset != 10 {
		t.Fatalf("%+v", parts)
	}
}

// Restart: members, generation, and committed offsets persist; sessions do
// not — every loaded member gets a full session-timeout grace period, so a
// broker restart never mass-evicts healthy consumers.
func TestGroupRestartKeepsMembership(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10)
	a1 := join(t, b1, "g1", "events", "m1")
	a2 := join(t, b1, "g1", "events", "m2")
	if a1.Generation != 1 || a2.Generation != 2 {
		t.Fatalf("%+v %+v", a1, a2)
	}
	if err := b1.CommitOffset("g1", "events", 0, 9, a2.Generation); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "groups.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"version": 2`, `"generation": 2`, `"m1"`, `"m2"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("groups.json missing %s: %s", want, data)
		}
	}
	if strings.Contains(string(data), `"size"`) {
		t.Fatalf("v2 file still carries the static size: %s", data)
	}
	b2 := openTestBroker(t, dir)
	// No re-join needed: membership and the cursor survived.
	parts := fetchOffsets(t, b2, "g1", "events", "m1")
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 {
		t.Fatalf("replayed after restart: %+v", parts[0])
	}
	// The persisted generation fences commits: stale fails, current works.
	if err := b2.CommitOffset("g1", "events", 0, 9, 1); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("err=%v", err)
	}
	if err := b2.CommitOffset("g1", "events", 0, 9, 2); err != nil {
		t.Fatal(err)
	}
	// Heartbeat works post-restart and returns the persisted assignment.
	if hb := heartbeat(t, b2, "g1", "events", "m2", 2); !slices.Equal(hb.Partitions, []int{1}) {
		t.Fatalf("%+v", hb)
	}
	// Sessions reset at Open: nobody is evicted inside the grace window...
	evicted, err := b2.ReapExpiredMembers(time.Now().Add(DefaultSessionTimeout - time.Second))
	if err != nil || evicted != 0 {
		t.Fatalf("evicted=%d err=%v", evicted, err)
	}
	// ...but members that never heartbeat after the restart do expire.
	evicted, err = b2.ReapExpiredMembers(time.Now().Add(2 * DefaultSessionTimeout))
	if err != nil || evicted != 2 {
		t.Fatalf("evicted=%d err=%v", evicted, err)
	}
}

// PLAN acceptance: consume 10, commit 9, restart, next fetch starts at 10.
// Membership and committed offsets survive the restart via meta/groups.json.
func TestGroupRestartNoReplay(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10)
	a := join(t, b1, "g1", "events", "m1")
	if got := fetchOffsets(t, b1, "g1", "events", "m1"); len(got[0].Records) != 10 {
		t.Fatalf("%+v", got[0])
	}
	if err := b1.CommitOffset("g1", "events", 0, 9, a.Generation); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "groups.json"))
	if err != nil || !strings.Contains(string(data), `"version": 2`) || !strings.Contains(string(data), `"m1"`) {
		t.Fatalf("groups.json: %q %v", data, err)
	}
	b2 := openTestBroker(t, dir)
	parts := fetchOffsets(t, b2, "g1", "events", "m1") // no re-join needed
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 {
		t.Fatalf("replayed after restart: %+v", parts[0])
	}
}

// Core acceptance: fetch from committed+1; committing the last processed
// offset makes the next fetch resume after it; rewinding replays.
func TestGroupFetchCommitResume(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 10)
	a := join(t, b, "g1", "events", "")
	parts := fetchOffsets(t, b, "g1", "events", "")
	if len(parts) != 1 || len(parts[0].Records) != 10 || parts[0].NextOffset != 10 || parts[0].HighWatermark != 10 {
		t.Fatalf("%+v", parts)
	}
	if parts[0].Records[0].Offset != 0 || parts[0].Records[9].Offset != 9 {
		t.Fatalf("%+v", parts[0].Records)
	}
	if err := b.CommitOffset("g1", "events", 0, 9, a.Generation); err != nil {
		t.Fatal(err)
	}
	parts = fetchOffsets(t, b, "g1", "events", "")
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 {
		t.Fatalf("%+v", parts[0])
	}
	if err := b.CommitOffset("g1", "events", 0, 4, a.Generation); err != nil { // rewind -> replay 5..9
		t.Fatal(err)
	}
	parts = fetchOffsets(t, b, "g1", "events", "")
	if len(parts[0].Records) != 5 || parts[0].Records[0].Offset != 5 || parts[0].NextOffset != 10 {
		t.Fatalf("%+v", parts[0])
	}
}

func TestCommitValidation(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 3)
	if err := b.CommitOffset("ghost", "events", 0, 0, 1); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	a := join(t, b, "g1", "events", "")
	if err := b.CommitOffset("g1", "ghost", 0, 0, a.Generation); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, 3, a.Generation); !errors.Is(err, ErrOffsetOutOfRange) { // == LEO
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, -1, a.Generation); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 1, 0, a.Generation); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("err=%v", err)
	}
}

// CommitOffset requires the CURRENT generation: stale generations are fenced
// (ErrGenerationFence), and every membership change invalidates the old one.
func TestCommitGenerationFence(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 3)
	a := join(t, b, "g1", "events", "m1")
	if err := b.CommitOffset("g1", "events", 0, 1, a.Generation+1); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, 1, a.Generation); err != nil {
		t.Fatal(err)
	}
	// A membership change invalidates the old generation for the NEXT commit.
	join(t, b, "g1", "events", "m2")
	if err := b.CommitOffset("g1", "events", 0, 2, a.Generation); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("err=%v", err)
	}
	if err := b.CommitOffset("g1", "events", 0, 2, a.Generation+1); err != nil {
		t.Fatal(err)
	}
}

// With several members the caller must name one; each member only ever sees
// its own partitions, and one member's commit never moves another's cursor.
func TestFetchGroupMemberResolution(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	join(t, b, "g1", "events", "m1")
	a2 := join(t, b, "g1", "events", "m2")
	produceN(t, b, "events", 0, 3)
	produceN(t, b, "events", 1, 5)
	if _, err := b.FetchGroup("g1", "events", "", 1000, 1<<20); !errors.Is(err, ErrMemberRequired) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.FetchGroup("g1", "events", "ghost", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := b.FetchGroup("ghost", "events", "m1", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	p1 := fetchOffsets(t, b, "g1", "events", "m1")
	if len(p1) != 1 || p1[0].Partition != 0 || len(p1[0].Records) != 3 {
		t.Fatalf("%+v", p1)
	}
	p2 := fetchOffsets(t, b, "g1", "events", "m2")
	if len(p2) != 1 || p2[0].Partition != 1 || len(p2[0].Records) != 5 {
		t.Fatalf("%+v", p2)
	}
	if err := b.CommitOffset("g1", "events", 0, 2, a2.Generation); err != nil {
		t.Fatal(err)
	}
	if got := fetchOffsets(t, b, "g1", "events", "m1"); len(got[0].Records) != 0 {
		t.Fatalf("%+v", got[0])
	}
	if got := fetchOffsets(t, b, "g1", "events", "m2"); len(got[0].Records) != 5 { // unaffected
		t.Fatalf("%+v", got[0])
	}
}

// A group that joined one topic has no cursor for another; an empty poll
// returns records: [] with next_offset pinned at the high watermark.
func TestFetchGroupEmptyAndUnknownTopic(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(TopicConfig{Name: "other", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	join(t, b, "g1", "events", "")
	if _, err := b.FetchGroup("g1", "other", "", 1000, 1<<20); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	parts := fetchOffsets(t, b, "g1", "events", "")
	if len(parts) != 1 || parts[0].Records == nil || len(parts[0].Records) != 0 || parts[0].NextOffset != 0 {
		t.Fatalf("%+v", parts)
	}
}

// --- Slice 6: lag reporting + the acks=0 crash-rewind interaction ---

// GroupLag: lag = latest - (committed+1); a partition the group never
// committed on reports committed=nil and lag = latest. Members and the
// generation are listed for demo readability; an unknown group filter is
// ErrGroupNotFound.
func TestGroupLag(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b, "events", 0, 10)
	produceN(t, b, "events", 1, 4)
	a := join(t, b, "g1", "events", "m1")
	if err := b.CommitOffset("g1", "events", 0, 6, a.Generation); err != nil {
		t.Fatal(err)
	}
	lags, err := b.GroupLag("")
	if err != nil || len(lags) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
	gl := lags[0]
	if gl.Group != "g1" || gl.Topic != "events" || gl.Generation != 1 || !slices.Equal(gl.Members, []string{"m1"}) {
		t.Fatalf("%+v", gl)
	}
	if len(gl.Partitions) != 2 {
		t.Fatalf("%+v", gl.Partitions)
	}
	if p0 := gl.Partitions[0]; p0.Committed == nil || *p0.Committed != 6 ||
		p0.NextFetch != 7 || p0.Latest != 10 || p0.Lag != 3 {
		t.Fatalf("%+v", p0)
	}
	if p1 := gl.Partitions[1]; p1.Committed != nil || p1.NextFetch != 0 ||
		p1.Latest != 4 || p1.Lag != 4 {
		t.Fatalf("%+v", p1)
	}
	if _, err := b.GroupLag("ghost"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err=%v", err)
	}
	if lags, err = b.GroupLag("g1"); err != nil || len(lags) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
}

// acks=0 records a group already committed can vanish in an OS crash,
// rewinding LEO BELOW committed+1 — simulated by truncating the log to a
// frame boundary while the broker is down (what losing unflushed page-cache
// pages looks like on disk). FetchGroup clamps to an empty poll at the new
// LEO: no error, and the committed offset is never rewritten. The clamp is
// a safety valve, not a recovery: the cursor stays authoritative, so the
// replacements later written at the lost offsets are NOT re-delivered (the
// group already committed their lost predecessors); delivery resumes at the
// first offset past the cursor. Negative lag is the visible signature of
// the rewind while it lasts (DURABILITY.md).
func TestFetchGroupLEORewindBelowCommitted(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10) // 22-byte frames: nil key, 1-byte value
	a := join(t, b1, "g1", "events", "")
	if got := fetchOffsets(t, b1, "g1", "events", ""); len(got[0].Records) != 10 {
		t.Fatalf("%+v", got[0])
	}
	if err := b1.CommitOffset("g1", "events", 0, 9, a.Generation); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	// "OS crash": the last 5 frames never reached the disk.
	logPath := filepath.Join(dir, "topics", "events", "0", segmentName(0))
	if err := os.Truncate(logPath, 5*22); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	if off, err := b2.GetOffsets("events", 0); err != nil || off.Latest != 5 {
		t.Fatalf("%+v %v", off, err)
	}
	// Empty poll at the rewound LEO — not an error, not a replay of 6..9.
	parts := fetchOffsets(t, b2, "g1", "events", "")
	if len(parts) != 1 || len(parts[0].Records) != 0 ||
		parts[0].NextOffset != 5 || parts[0].HighWatermark != 5 {
		t.Fatalf("%+v", parts)
	}
	lags, err := b2.GroupLag("g1")
	if err != nil || len(lags) != 1 || len(lags[0].Partitions) != 1 {
		t.Fatalf("%+v %v", lags, err)
	}
	if pl := lags[0].Partitions[0]; pl.Committed == nil || *pl.Committed != 9 ||
		pl.NextFetch != 10 || pl.Latest != 5 || pl.Lag != -5 {
		t.Fatalf("%+v", pl)
	}
	// Refill: new records reuse offsets 5..9, but the group's cursor is
	// already at 10 — it is NOT re-shown the replacements (its commit says
	// "processed through 9"). The poll stays empty until LEO passes 10.
	produceN(t, b2, "events", 0, 5)
	parts = fetchOffsets(t, b2, "g1", "events", "")
	if len(parts[0].Records) != 0 || parts[0].NextOffset != 10 || parts[0].HighWatermark != 10 {
		t.Fatalf("%+v", parts[0])
	}
	produceN(t, b2, "events", 0, 1) // offset 10: the first genuinely new record
	parts = fetchOffsets(t, b2, "g1", "events", "")
	if len(parts[0].Records) != 1 || parts[0].Records[0].Offset != 10 || parts[0].NextOffset != 11 {
		t.Fatalf("%+v", parts[0])
	}
	// The committed offset was never rewritten, and committing past the new
	// LEO still fails closed.
	if err := b2.CommitOffset("g1", "events", 0, 12, a.Generation); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
}

// --- Slice 7: persistence migration + the background reaper ---

// A v1 groups.json (Slice 4–6: static size, no generation) must not crash
// Open: members keep their join order, the size is discarded, and the
// generation starts at 1. The next membership change republishes as v2.
func TestGroupsFileV1Migration(t *testing.T) {
	dir := t.TempDir()
	b1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.CreateTopic(TopicConfig{Name: "events", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	produceN(t, b1, "events", 0, 10)
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	v1 := `{
  "version": 1,
  "groups": [
    {"name": "g1", "topics": [
      {"topic": "events", "size": 2, "members": ["a", "b"], "offsets": {"0": 5}}
    ]}
  ]
}
`
	if err := os.WriteFile(filepath.Join(dir, "meta", "groups.json"), []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}
	b2 := openTestBroker(t, dir)
	// Assignment derives from the live list: a (index 0) owns p0, b owns p1,
	// and a resumes from the migrated committed offset 5 → first record 6.
	parts := fetchOffsets(t, b2, "g1", "events", "a")
	if len(parts) != 1 || parts[0].Partition != 0 || len(parts[0].Records) != 4 || parts[0].Records[0].Offset != 6 {
		t.Fatalf("%+v", parts)
	}
	// The migrated generation is 1: it fences and commits like any other.
	if err := b2.CommitOffset("g1", "events", 0, 6, 2); !errors.Is(err, ErrGenerationFence) {
		t.Fatalf("err=%v", err)
	}
	if err := b2.CommitOffset("g1", "events", 0, 6, 1); err != nil {
		t.Fatal(err)
	}
	// The next membership change republishes the file as v2, size gone.
	join(t, b2, "g1", "events", "c")
	if err := b2.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta", "groups.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"version": 2`) || !strings.Contains(string(data), `"generation": 2`) ||
		strings.Contains(string(data), `"size"`) {
		t.Fatalf("not migrated forward: %s", data)
	}
}

// Unknown NEWER group file versions fail closed, like the topic catalog.
func TestGroupsFileUnknownVersionFails(t *testing.T) {
	dir := t.TempDir()
	if err := InitDirs(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta", "groups.json"),
		[]byte(`{"version": 3, "groups": []}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "unsupported groups version 3") {
		t.Fatalf("err=%v", err)
	}
}

// The background reaper really runs: with a short interval and timeout it
// evicts a member that never heartbeats, without the test ever calling
// ReapExpiredMembers.
func TestGroupReaperTickerRuns(t *testing.T) {
	b := openTestBroker(t, t.TempDir())
	if err := b.CreateTopic(TopicConfig{Name: "events", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	a := join(t, b, "g1", "events", "m1")
	b.SetSessionTimeout(20 * time.Millisecond)
	b.SetGroupReapInterval(10 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Poll GroupLag, NOT Heartbeat: a heartbeat would refresh the session
		// and keep the member alive forever.
		lags, err := b.GroupLag("g1")
		if err != nil {
			t.Fatal(err)
		}
		if len(lags) == 1 && len(lags[0].Members) == 0 && lags[0].Generation == a.Generation+1 {
			return // evicted by the ticker, generation bumped
		}
		if time.Now().After(deadline) {
			t.Fatalf("reaper never ran: %+v", lags)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
