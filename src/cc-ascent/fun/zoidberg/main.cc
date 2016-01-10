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

#include <string>
#include <iostream>

#include "proto-ascent/fun/bender/commands.pb.h"

int main(int argc, char *argv[]) {
  void protoMain();

  protoMain();
	std::cout << "Hooray! People are paying attention to me!" << std::endl;
	return 0;
}

void protoMain() {
	using namespace ascent::fun::bender;

	BenderCommand cmd;
  cmd.set_command_name("hello");

  std::string data;
  if (cmd.SerializeToString(&data) == false) {
    std::cerr << "protobuf serialization failed" << std::endl;
    return;
  }

  BenderCommand newCmd;
  if (newCmd.ParseFromString(data) == false) {
    std::cerr << "protobuf parsing failed" << std::endl;
    return;
  }

  if (cmd.command_name() != newCmd.command_name()) {
    std::cerr << "data mismatch " << cmd.command_name() << " != "
              << newCmd.command_name() << std::endl;
    return;
  }
}
