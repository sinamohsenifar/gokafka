package gokafka

// Pause stops fetching from the given assigned partitions until Resume is called.
func (c *Consumer) Pause(partitions ...TopicPartition) {
	if len(partitions) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused == nil {
		c.paused = map[partKey]struct{}{}
	}
	for _, p := range partitions {
		c.paused[partKey{p.Topic, p.Partition}] = struct{}{}
	}
}

// Resume restarts fetching from previously paused partitions.
func (c *Consumer) Resume(partitions ...TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.paused) == 0 {
		return
	}
	if len(partitions) == 0 {
		c.paused = nil
		return
	}
	for _, p := range partitions {
		delete(c.paused, partKey{p.Topic, p.Partition})
	}
	if len(c.paused) == 0 {
		c.paused = nil
	}
}

// PausedPartitions returns currently paused partitions.
func (c *Consumer) PausedPartitions() []TopicPartition {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.paused) == 0 {
		return nil
	}
	out := make([]TopicPartition, 0, len(c.paused))
	for _, a := range c.assignments {
		k := partKey{a.topic, a.partition}
		if _, ok := c.paused[k]; ok {
			out = append(out, TopicPartition{Topic: a.topic, Partition: a.partition, Offset: a.offset})
		}
	}
	return out
}

func (c *Consumer) isPaused(topic string, part int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.paused) == 0 {
		return false
	}
	_, ok := c.paused[partKey{topic, part}]
	return ok
}
