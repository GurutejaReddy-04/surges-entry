//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestProducerWritesAndPartitionAffinity(t *testing.T) {
	broker := envOr("KAFKA_BROKER", "localhost:9092")

	client, err := sarama.NewClient([]string{broker}, sarama.NewConfig())
	if err != nil {
		t.Skipf("kafka unavailable at %s: %v", broker, err)
	}
	client.Close()

	testTopic := fmt.Sprintf("test-affinity-%d", time.Now().UnixNano())

	producer, err := newProducer(broker)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	// Send 20 messages (5 users × 4 rounds), tracking which partition each user lands on.
	// If the same user ever lands on a different partition, the key-based partitioning is broken.
	users := buildUserPool(5)
	partitionOf := make(map[string]int32)
	const rounds = 4

	for round := range rounds {
		for _, uid := range users {
			ev := Event{
				UserID:    uid,
				EventType: "test",
				Value:     42.0,
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			}
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			msg := &sarama.ProducerMessage{
				Topic: testTopic,
				Key:   sarama.StringEncoder(ev.UserID),
				Value: sarama.ByteEncoder(data),
			}

			partition, _, err := producer.SendMessage(msg)
			if err != nil {
				t.Fatalf("send %s round %d: %v", uid, round, err)
			}

			if prev, seen := partitionOf[uid]; seen && prev != partition {
				t.Errorf("partition affinity broken: %s was on %d, now on %d", uid, prev, partition)
			}
			partitionOf[uid] = partition
		}
	}

	t.Logf("partition map: %v", partitionOf)

	// Consume everything back. If we don't get exactly 20 messages, something was lost.
	consumer, err := sarama.NewConsumer([]string{broker}, sarama.NewConfig())
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(testTopic)
	if err != nil {
		t.Fatalf("list partitions for %s: %v", testTopic, err)
	}

	var total int
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(testTopic, p, sarama.OffsetOldest)
		if err != nil {
			t.Fatalf("consume partition %d: %v", p, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	drain:
		for {
			select {
			case msg, ok := <-pc.Messages():
				if !ok {
					break drain
				}
				var ev Event
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					t.Errorf("unmarshal consumed message: %v", err)
					continue
				}
				if expected := partitionOf[ev.UserID]; msg.Partition != expected {
					t.Errorf("consumed %s from partition %d, expected %d",
						ev.UserID, msg.Partition, expected)
				}
				total++
			case <-ctx.Done():
				break drain
			}
		}
		cancel()
		pc.Close()
	}

	want := len(users) * rounds
	if total != want {
		t.Errorf("consumed %d messages, want %d", total, want)
	}
}
