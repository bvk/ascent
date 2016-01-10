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
// This file defines Int64Slice type similar to StringSlice from 'sort'
// package in Go standard library.
//

package fswal

type Int64Slice []int64

func (this Int64Slice) Len() int {
	return len([]int64(this))
}

func (this Int64Slice) Less(i, j int) bool {
	return this[i] < this[j]
}

func (this Int64Slice) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
