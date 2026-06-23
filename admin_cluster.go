package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// ClusterBroker describes a broker from DescribeCluster.
type ClusterBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// ClusterDescription is cluster-wide metadata from the controller/broker.
type ClusterDescription struct {
	ClusterID    string
	ControllerID int32
	Brokers      []ClusterBroker
	ErrorCode    ErrorCode
}

// DescribeCluster returns cluster id, controller, and registered brokers.
// Uses DescribeCluster API (60) when supported, falling back to Metadata.
func (a *Admin) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	if err := a.client.requireOpen(); err != nil {
		return ClusterDescription{}, err
	}
	if desc, err := a.describeClusterWire(ctx); err == nil {
		return desc, nil
	}
	return a.describeClusterFromMetadata(ctx)
}

func (a *Admin) describeClusterWire(ctx context.Context) (ClusterDescription, error) {
	body := protocol.EncodeDescribeClusterRequest()
	rb, err := a.client.cluster.RequestViaSeed(ctx, protocol.APIDescribeCluster, protocol.VerDescribeCluster, body)
	if err != nil {
		return ClusterDescription{}, err
	}
	wire, err := protocol.DecodeDescribeClusterResponse(rb)
	if err != nil {
		return ClusterDescription{}, err
	}
	if wire.ErrorCode != 0 {
		return ClusterDescription{}, newKafkaError(wire.ErrorCode, "", 0, "describe cluster failed")
	}
	out := ClusterDescription{
		ClusterID: wire.ClusterID, ControllerID: wire.ControllerID, ErrorCode: ErrorCode(wire.ErrorCode),
	}
	for _, b := range wire.Brokers {
		out.Brokers = append(out.Brokers, ClusterBroker{
			NodeID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack,
		})
	}
	return out, nil
}

func (a *Admin) describeClusterFromMetadata(ctx context.Context) (ClusterDescription, error) {
	if err := a.client.cluster.Refresh(ctx, nil); err != nil {
		return ClusterDescription{}, err
	}
	meta := a.client.cluster.Metadata()
	out := ClusterDescription{
		ClusterID: meta.ClusterID, ControllerID: meta.Controller,
	}
	for _, b := range meta.Brokers {
		out.Brokers = append(out.Brokers, ClusterBroker{
			NodeID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack,
		})
	}
	return out, nil
}
