package protocol

// Kafka request API keys (see kafka.apache.org/protocol).
const (
	APIProduce                 int16 = 0
	APIFetch                   int16 = 1
	APIListOffsets             int16 = 2
	APIMetadata                int16 = 3
	APIOffsetCommit            int16 = 8
	APIOffsetFetch             int16 = 9
	APIFindCoordinator         int16 = 10
	APIJoinGroup               int16 = 11
	APIHeartbeat               int16 = 12
	APILeaveGroup              int16 = 13
	APISyncGroup               int16 = 14
	APICreateTopics            int16 = 19
	APIDeleteTopics            int16 = 20
	APIApiVersions             int16 = 18
	APIListGroups              int16 = 16
	APIDescribeGroups          int16 = 15
	APIDescribeConfigs         int16 = 32
	APIAlterConfigs            int16 = 33
	APICreatePartitions        int16 = 37
	APIDescribeLogDirs         int16 = 35
	APIDeleteRecords           int16 = 21
	APIDeleteGroups            int16 = 42
	APIElectLeaders            int16 = 43
	APIIncrementalAlterConfigs int16 = 44
	APIAlterPartitionReassign  int16 = 45
	APIListPartitionReassign   int16 = 46
	APIOffsetDelete            int16 = 47
	APIDescribeClientQuotas    int16 = 48
	APIAlterClientQuotas       int16 = 49
	APIDescribeUserScramCreds  int16 = 50
	APIAlterUserScramCreds     int16 = 51
	APIDescribeTransactions    int16 = 65
	APIListTransactions        int16 = 66
	APISaslHandshake           int16 = 17
	APISaslAuthenticate        int16 = 36
	APIConsumerGroupHeartbeat  int16 = 68
	APIConsumerGroupDescribe   int16 = 69
	APIShareGroupHeartbeat     int16 = 76
	APIShareGroupDescribe      int16 = 77
	APIShareFetch              int16 = 78
	APIShareAcknowledge        int16 = 79
)

// Negotiated API version caps (client max; broker may be lower).
const (
	VerMetadata                int16 = 12
	VerProduce                 int16 = 12
	VerFetch                   int16 = 13
	VerListOffsets             int16 = 3
	VerOffsetCommit            int16 = 8
	VerOffsetFetch             int16 = 9 // negotiation ceiling (KIP-709 batched multi-group is v8+)
	VerOffsetFetchSingle       int16 = 7 // single-group path the consumer pins (v7 adds require_stable, KIP-447)
	VerOffsetFetchMultiGroup   int16 = 8 // batched multi-group OffsetFetch (KIP-709)
	VerFindCoordinator         int16 = 3 // flexible (KIP-482 tagged fields); v3+ on all Kafka 3.4+ targets
	VerJoinGroup               int16 = 6
	VerSyncGroup               int16 = 5
	VerHeartbeat               int16 = 4
	VerLeaveGroup              int16 = 5
	VerCreateTopics            int16 = 4
	VerDeleteTopics            int16 = 6
	VerListGroups              int16 = 2
	VerDescribeGroups          int16 = 5
	VerDescribeConfigs         int16 = 4
	VerAlterConfigs            int16 = 2
	VerCreatePartitions        int16 = 2
	VerDescribeLogDirs         int16 = 5
	VerDeleteRecords           int16 = 2
	VerElectLeaders            int16 = 2
	VerAlterPartitionReassign  int16 = 0
	VerListPartitionReassign   int16 = 0
	VerDescribeClientQuotas    int16 = 1
	VerAlterClientQuotas       int16 = 1
	VerDescribeUserScramCreds  int16 = 0
	VerAlterUserScramCreds     int16 = 0
	VerDescribeTransactions    int16 = 0
	VerListTransactions        int16 = 2
	VerDeleteGroups            int16 = 2
	VerIncrementalAlterConfigs int16 = 0
	VerOffsetDelete            int16 = 0
	VerApiVersions             int16 = 3
	VerSaslHandshake           int16 = 1
	VerSaslAuthenticate        int16 = 1
	VerConsumerGroupHeartbeat  int16 = 1
	VerShareGroupHeartbeat     int16 = 1
	VerShareGroupDescribe      int16 = 1
	VerShareFetch              int16 = 2
	VerShareAcknowledge        int16 = 2 // v2 adds is_renew_ack (KIP-1222); negotiated down to v1 on older brokers
)
