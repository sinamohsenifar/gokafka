package protocol

func flexibleRequestHeader(apiKey, apiVersion int16) bool {
	switch apiKey {
	case APIMetadata:
		return apiVersion >= 9
	case APIApiVersions:
		return apiVersion >= 3
	case APIProduce:
		return apiVersion >= 9
	case APIFetch:
		return apiVersion >= 12
	case APIListOffsets:
		return apiVersion >= 6
	case APIOffsetCommit:
		return apiVersion >= 8
	case APIOffsetFetch:
		return apiVersion >= 6
	case APIFindCoordinator:
		return apiVersion >= 3
	case APIJoinGroup:
		return apiVersion >= 6
	case APISyncGroup:
		return apiVersion >= 4
	case APIHeartbeat, APILeaveGroup:
		return apiVersion >= 4
	case APICreateTopics:
		return apiVersion >= 5
	case APIDeleteTopics:
		return apiVersion >= 4
	case APIListGroups:
		return apiVersion >= 3
	case APIDescribeGroups:
		return apiVersion >= 5
	case APIDescribeConfigs:
		return apiVersion >= 4
	case APIAlterConfigs:
		return apiVersion >= 2
	case APICreatePartitions, APIDeleteGroups:
		return apiVersion >= 2
	case APIIncrementalAlterConfigs:
		return apiVersion >= 1
	case APICreateAcls, APIDescribeAcls, APIDeleteAcls:
		return apiVersion >= 2
	case APISaslAuthenticate:
		return apiVersion >= 3
	case APIDescribeCluster:
		return apiVersion >= 1
	case APIInitProducerID:
		return apiVersion >= 2
	case APIAddOffsetsToTxn, APITxnOffsetCommit:
		return apiVersion >= 3
	case APIAddPartitionsTxn:
		return apiVersion >= 3
	case 	APIEndTxn:
		return apiVersion >= 3
	case APIConsumerGroupHeartbeat:
		return apiVersion >= 0
	default:
		return false
	}
}
