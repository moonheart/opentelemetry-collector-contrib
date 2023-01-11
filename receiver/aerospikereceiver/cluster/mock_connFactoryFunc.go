// Code generated by mockery v2.13.1. DO NOT EDIT.

package cluster

import (
	aerospike "github.com/aerospike/aerospike-client-go/v5"
	mock "github.com/stretchr/testify/mock"
)

// mockConnFactoryFunc is an autogenerated mock type for the connFactoryFunc type
type mockConnFactoryFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0, _a1
func (_m *mockConnFactoryFunc) Execute(_a0 *aerospike.ClientPolicy, _a1 *aerospike.Host) (asconn, aerospike.Error) {
	ret := _m.Called(_a0, _a1)

	var r0 asconn
	if rf, ok := ret.Get(0).(func(*aerospike.ClientPolicy, *aerospike.Host) asconn); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(asconn)
		}
	}

	var r1 aerospike.Error
	if rf, ok := ret.Get(1).(func(*aerospike.ClientPolicy, *aerospike.Host) aerospike.Error); ok {
		r1 = rf(_a0, _a1)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}

	return r0, r1
}

type mockConstructorTestingTnewMockConnFactoryFunc interface {
	mock.TestingT
	Cleanup(func())
}

// newMockConnFactoryFunc creates a new instance of mockConnFactoryFunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func newMockConnFactoryFunc(t mockConstructorTestingTnewMockConnFactoryFunc) *mockConnFactoryFunc {
	mock := &mockConnFactoryFunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
