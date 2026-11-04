// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Stop at the RPC boundary so these tests check command dispatch without a
// controller or any actual accounting mutations.
var errRecordedRequest = errors.New("request recorded")

type accountingRequestRecorder struct {
	protos.CraneCtldClient
	request proto.Message
}

func (c *accountingRequestRecorder) QueryAccountInfo(_ context.Context, req *protos.QueryAccountInfoRequest, _ ...grpc.CallOption) (*protos.QueryAccountInfoReply, error) {
	c.request = req
	return nil, errRecordedRequest
}
func (c *accountingRequestRecorder) QueryUserInfo(_ context.Context, req *protos.QueryUserInfoRequest, _ ...grpc.CallOption) (*protos.QueryUserInfoReply, error) {
	c.request = req
	return nil, errRecordedRequest
}
func (c *accountingRequestRecorder) QueryQosInfo(_ context.Context, req *protos.QueryQosInfoRequest, _ ...grpc.CallOption) (*protos.QueryQosInfoReply, error) {
	c.request = req
	return nil, errRecordedRequest
}
func (c *accountingRequestRecorder) DeleteAccount(_ context.Context, req *protos.DeleteAccountRequest, _ ...grpc.CallOption) (*protos.DeleteAccountReply, error) {
	c.request = req
	return nil, errRecordedRequest
}
func (c *accountingRequestRecorder) DeleteUser(_ context.Context, req *protos.DeleteUserRequest, _ ...grpc.CallOption) (*protos.DeleteUserReply, error) {
	c.request = req
	return nil, errRecordedRequest
}
func (c *accountingRequestRecorder) DeleteQos(_ context.Context, req *protos.DeleteQosRequest, _ ...grpc.CallOption) (*protos.DeleteQosReply, error) {
	c.request = req
	return nil, errRecordedRequest
}

func TestNameFiltersReachAccountingRequests(t *testing.T) {
	oldStub, oldUid, oldForce := stub, userUid, FlagForce
	t.Cleanup(func() { stub, userUid, FlagForce = oldStub, oldUid, oldForce })
	userUid, FlagForce = 1234, false
	for _, tc := range []struct {
		input string
		want  proto.Message
	}{
		{"show account Name=acct1,acct2", &protos.QueryAccountInfoRequest{Uid: 1234, AccountList: []string{"acct1", "acct2"}}},
		{"show account acct1", &protos.QueryAccountInfoRequest{Uid: 1234, AccountList: []string{"acct1"}}},
		{"show account where Name=acct1", &protos.QueryAccountInfoRequest{Uid: 1234, AccountList: []string{"acct1"}}},
		{"show user Account=acct1 Name=user1,user2", &protos.QueryUserInfoRequest{Uid: 1234, UserList: []string{"user1", "user2"}, Account: "acct1"}},
		{"show user where Name=user1 Account=acct1", &protos.QueryUserInfoRequest{Uid: 1234, UserList: []string{"user1"}, Account: "acct1"}},
		{"show user user1 Account=acct1", &protos.QueryUserInfoRequest{Uid: 1234, UserList: []string{"user1"}, Account: "acct1"}},
		{"show qos Name=normal", &protos.QueryQosInfoRequest{Uid: 1234, QosList: []string{"normal"}}},
		{"show qos where Name=normal", &protos.QueryQosInfoRequest{Uid: 1234, QosList: []string{"normal"}}},
		{"delete account Name=acct1,acct2", &protos.DeleteAccountRequest{Uid: 1234, AccountList: []string{"acct1", "acct2"}}},
		{"delete user Name=user1 Account=acct1", &protos.DeleteUserRequest{Uid: 1234, UserList: []string{"user1"}, Account: "acct1"}},
		{"delete qos Name=normal", &protos.DeleteQosRequest{Uid: 1234, QosList: []string{"normal"}}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			command, err := ParseCAcctMgrCommand(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			recorder := &accountingRequestRecorder{}
			stub = recorder
			_ = actionToExecute[command.GetAction()](command)
			if !proto.Equal(recorder.request, tc.want) {
				t.Fatalf("request = %v, want %v", recorder.request, tc.want)
			}
		})
	}
}

func TestDeleteStillRequiresName(t *testing.T) {
	for _, entity := range []string{"account", "user", "qos"} {
		command, err := ParseCAcctMgrCommand("delete " + entity)
		if err != nil {
			t.Fatal(err)
		}
		if err := executeDeleteCommand(command); err == nil {
			t.Fatalf("delete %s without a name succeeded", entity)
		}
	}
}
