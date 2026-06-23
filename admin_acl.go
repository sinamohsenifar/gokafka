package gokafka

import (
	"context"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

type (
	ACLResourceType   = protocol.ResourceType
	ACLOperation      = protocol.ACLOperation
	ACLPermissionType = protocol.ACLPermissionType
	ACLBinding        = protocol.ACLBinding
)

const (
	ACLResourceTopic   = protocol.ResourceTopic
	ACLResourceGroup   = protocol.ResourceGroup
	ACLResourceCluster = protocol.ResourceCluster

	ACLOperationRead   = protocol.ACLOpRead
	ACLOperationWrite  = protocol.ACLOpWrite
	ACLPermissionAllow = protocol.ACLPermAllow
	ACLPermissionDeny  = protocol.ACLPermDeny
)

// CreateACLs creates ACL bindings on the cluster.
func (a *Admin) CreateACLs(ctx context.Context, bindings ...ACLBinding) error {
	ver := a.client.cluster.NegotiatedVersion(protocol.APICreateAcls, protocol.VerCreateAcls)
	if ver < 0 {
		ver = protocol.VerCreateAcls
	}
	body := protocol.EncodeCreateACLsRequest(ver, bindings)
	resp, err := a.requestAny(ctx, protocol.APICreateAcls, ver, body)
	if err != nil {
		return err
	}
	results, err := protocol.DecodeCreateACLsResponse(ver, resp)
	if err != nil {
		return err
	}
	for i, r := range results {
		if r.ErrorCode != 0 {
			return newKafkaError(r.ErrorCode, "", 0, fmt.Sprintf("create acl binding %d failed", i))
		}
	}
	return nil
}

// DescribeACLs lists ACL bindings matching the filter.
func (a *Admin) DescribeACLs(ctx context.Context, resourceType ACLResourceType, name, principal string) ([]ACLBinding, error) {
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDescribeAcls, protocol.VerDescribeAcls)
	if ver < 0 {
		ver = protocol.VerDescribeAcls
	}
	body := protocol.EncodeDescribeACLsFilter(ver, resourceType, name, principal)
	resp, err := a.requestAny(ctx, protocol.APIDescribeAcls, ver, body)
	if err != nil {
		return nil, err
	}
	return protocol.DecodeDescribeACLsResponse(ver, resp)
}

// DeleteACLs removes ACL bindings matching the filter.
func (a *Admin) DeleteACLs(ctx context.Context, resourceType ACLResourceType, name, principal string) (int, error) {
	if name == "" && principal == "" {
		return 0, fmt.Errorf("gokafka: DeleteACLs requires non-empty resource name or principal (use \"*\" to match all)")
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDeleteAcls, protocol.VerDeleteAcls)
	if ver < 0 {
		ver = protocol.VerDeleteAcls
	}
	body := protocol.EncodeDeleteACLsFilter(ver, resourceType, name, principal)
	resp, err := a.requestAny(ctx, protocol.APIDeleteAcls, ver, body)
	if err != nil {
		return 0, err
	}
	return protocol.DecodeDeleteACLsResponse(resp)
}
