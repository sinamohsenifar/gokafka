package protocol

// Kafka request API keys (see kafka.apache.org/protocol).
const (
	APIProduce          int16 = 0
	APIFetch            int16 = 1
	APIListOffsets      int16 = 2
	APIMetadata         int16 = 3
	APIOffsetCommit     int16 = 8
	APIOffsetFetch      int16 = 9
	APIFindCoordinator  int16 = 10
	APIJoinGroup        int16 = 11
	APIHeartbeat        int16 = 12
	APILeaveGroup       int16 = 13
	APISyncGroup        int16 = 14
	APICreateTopics     int16 = 19
	APIDeleteTopics     int16 = 20
	APIApiVersions      int16 = 18
	APIListGroups       int16 = 16
	APIDescribeGroups   int16 = 15
	APIDescribeConfigs  int16 = 32
	APIAlterConfigs             int16 = 33
	APICreatePartitions         int16 = 37
	APIDeleteGroups             int16 = 42
	APIIncrementalAlterConfigs  int16 = 44
	APIOffsetDelete             int16 = 47
	APISaslHandshake    int16 = 17
	APISaslAuthenticate int16 = 36
	APIConsumerGroupHeartbeat int16 = 68
	APIConsumerGroupDescribe  int16 = 69
)

// Negotiated API version caps (client max; broker may be lower).
const (
	VerMetadata         int16 = 12
	VerProduce          int16 = 7
	VerFetch            int16 = 11
	VerListOffsets      int16 = 3
	VerOffsetCommit     int16 = 7
	VerOffsetFetch      int16 = 5
	VerFindCoordinator  int16 = 1
	VerJoinGroup        int16 = 5
	VerSyncGroup        int16 = 3
	VerHeartbeat        int16 = 1
	VerLeaveGroup       int16 = 2
	VerCreateTopics     int16 = 4
	VerDeleteTopics     int16 = 6
	VerListGroups       int16 = 2
	VerDescribeGroups   int16 = 4
	VerDescribeConfigs  int16 = 4
	VerAlterConfigs            int16 = 1
	VerCreatePartitions        int16 = 2
	VerDeleteGroups            int16 = 2
	VerIncrementalAlterConfigs int16 = 0
	VerOffsetDelete            int16 = 0
	VerApiVersions      int16 = 2
	VerSaslHandshake    int16 = 1
	VerSaslAuthenticate int16 = 1
	VerConsumerGroupHeartbeat int16 = 1
)
