package protocol

// safePrealloc bounds a slice preallocation derived from an untrusted wire
// array count. A corrupt or hostile frame can advertise a huge element count;
// capping the initial make() prevents a multi-gigabyte allocation before the
// decode loop (which reads element-by-element and errors on a short buffer)
// even runs. Legitimately large arrays still grow via append.
func safePrealloc(n int) int {
	const maxPrealloc = 4096
	if n <= 0 {
		return 0
	}
	if n > maxPrealloc {
		return maxPrealloc
	}
	return n
}

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
	case APIDeleteRecords:
		return apiVersion >= 2
	case APIElectLeaders:
		return apiVersion >= 2
	case APIDescribeLogDirs:
		return apiVersion >= 2
	case APIDescribeClientQuotas, APIAlterClientQuotas:
		return apiVersion >= 1
	case APIAlterUserScramCreds:
		return apiVersion >= 0
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
	case APIEndTxn:
		return apiVersion >= 3
	case APIConsumerGroupHeartbeat:
		return apiVersion >= 0
	case APIShareGroupHeartbeat, APIShareGroupDescribe, APIShareFetch, APIShareAcknowledge:
		return apiVersion >= 1
	default:
		return false
	}
}
