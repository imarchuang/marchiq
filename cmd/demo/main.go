// Command demo is the marchiq teaching script: a stdlib-only producer and
// consumer for the graduation demo (PLAN.md). It is deliberately NOT a
// client library — no retries, no batching, one HTTP request per operation,
// so every wire round-trip stays visible.
//
//	go run ./cmd/demo -mode produce -topic events -n 100 -keys 10
//	go run ./cmd/demo -mode consume -topic events -group workers -member w1 -members 2
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "", "produce or consume (required)")
	addr := flag.String("addr", "http://localhost:9092", "broker base URL")
	topic := flag.String("topic", "events", "topic name")
	n := flag.Int("n", 100, "produce: number of records to send")
	keys := flag.Int("keys", 10, "produce: distinct keys k1..kK; 0 sends keyless records (round-robin)")
	acks := flag.Int("acks", 1, "produce: 1 waits for fsync, 0 returns after the page-cache write")
	group := flag.String("group", "workers", "consume: consumer group")
	member := flag.String("member", "", "consume: member id (default: demo-<pid>)")
	members := flag.Int("members", 1, "consume: declared group size (fixed by the first join)")
	noCommit := flag.Bool("noCommit", false, "consume: fetch and print but never commit (restarting replays everything)")
	flag.Parse()

	if *member == "" {
		*member = fmt.Sprintf("demo-%d", os.Getpid())
	}
	var err error
	switch *mode {
	case "produce":
		err = runProduce(*addr, *topic, *n, *keys, *acks)
	case "consume":
		err = runConsume(*addr, *topic, *group, *member, *members, *noCommit)
	default:
		fmt.Fprintf(os.Stderr, "unknown -mode %q (want produce|consume)\n", *mode)
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

// req issues one HTTP call and returns the status and body. Non-2xx is an
// error carrying the broker's message — the demo fails loudly, never guesses.
func req(client *http.Client, method, url string, body io.Reader) ([]byte, error) {
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, bytes.TrimSpace(data))
	}
	return data, nil
}

// runProduce sends -n records with partition=-1, cycling keys k1..kK, then
// prints the per-partition histogram and the key→partition placement: the
// two things the demo exists to show (spread, and same-key stickiness).
func runProduce(addr, topic string, n, keys, acks int) error {
	if n < 1 || keys < 0 {
		return fmt.Errorf("n must be >= 1 and keys >= 0")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	perPartition := map[int]int{}
	keyPlacement := map[string]int{}
	for i := 0; i < n; i++ {
		key := ""
		if keys > 0 {
			key = fmt.Sprintf("k%d", i%keys+1)
		}
		u := fmt.Sprintf("%s/produce?topic=%s&partition=-1&acks=%d&key=%s",
			addr, url.QueryEscape(topic), acks, url.QueryEscape(key))
		data, err := req(client, "POST", u, bytes.NewReader([]byte(fmt.Sprintf("payload-%d", i))))
		if err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
		var ack struct {
			Partition int   `json:"partition"`
			Offset    int64 `json:"offset"`
		}
		if err := json.Unmarshal(data, &ack); err != nil {
			return fmt.Errorf("record %d: bad response: %w", i, err)
		}
		perPartition[ack.Partition]++
		if key != "" {
			if prev, seen := keyPlacement[key]; seen && prev != ack.Partition {
				return fmt.Errorf("key %s moved partitions: %d then %d (partitioner broken)", key, prev, ack.Partition)
			}
			keyPlacement[key] = ack.Partition
		}
	}
	fmt.Printf("produced %d records (acks=%d)\n", n, acks)
	parts := make([]int, 0, len(perPartition))
	for p := range perPartition {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	for _, p := range parts {
		fmt.Printf("partition %d: %d records\n", p, perPartition[p])
	}
	if len(keyPlacement) > 0 {
		ks := make([]string, 0, len(keyPlacement))
		for k := range keyPlacement {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		fmt.Print("key placement:")
		for _, k := range ks {
			fmt.Printf(" %s→p%d", k, keyPlacement[k])
		}
		fmt.Println()
	}
	return nil
}

// runConsume joins the group, then loops fetch → print → commit-last-offset
// per partition until Ctrl-C. With -noCommit it never commits, so a restart
// replays from the last committed offset (at-least-once, made visible).
func runConsume(addr, topic, group, member string, members int, noCommit bool) error {
	client := &http.Client{Timeout: 10 * time.Second}
	join := fmt.Sprintf("%s/groups/%s/join?topic=%s&member=%s&members=%d",
		addr, url.PathEscape(group), url.QueryEscape(topic), url.QueryEscape(member), members)
	data, err := req(client, "POST", join, nil)
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	var asg struct {
		Partitions []int `json:"partitions"`
	}
	if err := json.Unmarshal(data, &asg); err != nil {
		return fmt.Errorf("join: bad response: %w", err)
	}
	fmt.Printf("joined group=%s member=%s topic=%s partitions=%v (commit=%v)\n",
		group, member, topic, asg.Partitions, !noCommit)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fetchURL := fmt.Sprintf("%s/fetch?group=%s&topic=%s&member=%s",
		addr, url.QueryEscape(group), url.QueryEscape(topic), url.QueryEscape(member))
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			fmt.Printf("\nstopped; consumed %d records\n", total)
			return nil
		}
		data, err := req(client, "GET", fetchURL, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fetch:", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		var resp struct {
			Partitions []struct {
				Partition int `json:"partition"`
				Records   []struct {
					Offset int64  `json:"offset"`
					Key    []byte `json:"key"`
					Value  []byte `json:"value"`
				} `json:"records"`
				NextOffset int64 `json:"next_offset"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return fmt.Errorf("fetch: bad response: %w", err)
		}
		got := 0
		for _, p := range resp.Partitions {
			for _, rec := range p.Records {
				fmt.Printf("p%d @%d %s=%s\n", p.Partition, rec.Offset, rec.Key, rec.Value)
				got++
				total++
			}
			// Commit the last PROCESSED offset (next_offset-1); the next
			// fetch resumes after it. Commit after printing = at-least-once.
			if !noCommit && len(p.Records) > 0 {
				commit := map[string]any{
					"group": group, "topic": topic,
					"partition": p.Partition, "offset": p.NextOffset - 1,
				}
				body, _ := json.Marshal(commit)
				if _, err := req(client, "POST", addr+"/commit", bytes.NewReader(body)); err != nil {
					return fmt.Errorf("commit: %w", err)
				}
			}
		}
		if got == 0 && !sleep(ctx, 500*time.Millisecond) {
			fmt.Printf("\nstopped; consumed %d records\n", total)
			return nil
		}
	}
}

// sleep waits d or until the context is cancelled; false means "stop".
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
