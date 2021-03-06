// Copyright (c) 2016 BVK Chaitanya
//
// This file is part of the Ascent Library.
//
// The Ascent Library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Ascent Library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with the Ascent Library.  If not, see <http://www.gnu.org/licenses/>.

//
// A toy program for testing purposes.
//

package main

import (
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"

	"proto-ascent/fun/bender"
)

func main() {
	protoMain()
	fmt.Println("Hello Meatbags!")
}

func protoMain() {
	cmd := &bender.BenderCommand{
		CommandName: proto.String("hello"),
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	newCmd := &bender.BenderCommand{}
	err = proto.Unmarshal(data, newCmd)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}
	// Now test and newTest contain the same data.
	if cmd.GetCommandName() != newCmd.GetCommandName() {
		log.Fatalf("data mismatch %q != %q", cmd.GetCommandName(),
			newCmd.GetCommandName())
	}
}
