/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula

import (
    "../../../interface/gen-go/nebula/graph"
    "github.com/facebook/fbthrift/thrift/lib/go/thrift"
    "log"
)

var logger *log.Logger

type GraphClient struct {
    graph graph.GraphServiceClient
}

func NewClient(address string) (client *GraphClient, err error) {
    timeoutOption := thrift.SocketTimeout(1000 * 3)
    addressOption := thrift.SocketAddr(address)
    transport, err := thrift.NewSocket(timeoutOption, addressOption)

    if err != nil {
        logger.Printf("NewClient Failed : %v", err)
        return nil, err
    }

    protocol := thrift.NewBinaryProtocolFactoryDefault()
    graph := &GraphClient{
        graph: *graph.NewGraphServiceClientFactory(transport, protocol),
    }
    return graph, nil
}

func (client *GraphClient) Authenticate(username string, password string) (response *graph.AuthResponse, err error) {
    if response, err := client.graph.Authenticate(username, password); err != nil {
        logger.Printf("Authenticate Failed : %v", err)
        return nil, err
    } else {
        return response, nil
    }
}

func (client *GraphClient) Signout(sessionID int64) {
    if err := client.graph.Signout(sessionID); err != nil {
        logger.Printf("Signout Failed : %v", err)
    }
}

func (client *GraphClient) execute(sessionID int64, stmt string) (response *graph.ExecutionResponse, err error) {
    if response, err := client.graph.Execute(sessionID, stmt); err != nil {
        logger.Printf("Execute Failed : %v", err)
        return nil, err
    } else {
        return response, nil
    }
}

