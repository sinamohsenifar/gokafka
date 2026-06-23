package protocol

// ClientVersion returns the highest protocol version GoKafka implements for an API key.
func ClientVersion(apiKey int16) int16 {
	switch apiKey {
	case APIProduce:
		return VerProduce
	case APIFetch:
		return VerFetch
	case APIListOffsets:
		return VerListOffsets
	case APIMetadata:
		return VerMetadata
	case APIOffsetCommit:
		return VerOffsetCommit
	case APIOffsetFetch:
		return VerOffsetFetch
	case APIFindCoordinator:
		return VerFindCoordinator
	case APIJoinGroup:
		return VerJoinGroup
	case APIHeartbeat:
		return VerHeartbeat
	case APILeaveGroup:
		return VerLeaveGroup
	case APISyncGroup:
		return VerSyncGroup
	case APICreateTopics:
		return VerCreateTopics
	case APIDeleteTopics:
		return VerDeleteTopics
	case APIApiVersions:
		return VerApiVersions
	case APIListGroups:
		return VerListGroups
	case APIDescribeGroups:
		return VerDescribeGroups
	case APIDescribeConfigs:
		return VerDescribeConfigs
	case APIAlterConfigs:
		return VerAlterConfigs
	case APICreatePartitions:
		return VerCreatePartitions
	case APIDeleteGroups:
		return VerDeleteGroups
	case APIIncrementalAlterConfigs:
		return VerIncrementalAlterConfigs
	case APIOffsetDelete:
		return VerOffsetDelete
	case APICreateAcls:
		return VerCreateAcls
	case APIDescribeAcls:
		return VerDescribeAcls
	case APIDeleteAcls:
		return VerDeleteAcls
	case APIDescribeCluster:
		return VerDescribeCluster
	case APISaslHandshake:
		return VerSaslHandshake
	case APISaslAuthenticate:
		return VerSaslAuthenticate
	case APIInitProducerID:
		return VerInitProducerID
	case APIAddOffsetsToTxn:
		return VerAddOffsetsToTxn
	case APITxnOffsetCommit:
		return VerTxnOffsetCommit
	case APIAddPartitionsTxn:
		return VerAddPartitionsTxn
	case APIEndTxn:
		return VerEndTxn
	case APIConsumerGroupHeartbeat:
		return VerConsumerGroupHeartbeat
	default:
		return 0
	}
}
