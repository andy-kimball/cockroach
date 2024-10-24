// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build go1.9

package engine

import "unsafe" // for go:linkname

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the rawbyteslice function. Note that this
// access is necessarily tied to a specific Go release which is why this file
// is protected by a build tag.

//go:linkname rawbyteslice runtime.rawbyteslice
func rawbyteslice(size int) (b []byte)

// Replacement for C.GoBytes which does not zero initialize the returned slice
// before overwriting it. See https://github.com/golang/go/issues/23634.
func gobytes(ptr unsafe.Pointer, len int) []byte {
	x := rawbyteslice(len)
	src := (*[maxArrayLen]byte)(ptr)[:len:len]
	copy(x, src)
	return x
}
