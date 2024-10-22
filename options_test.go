// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beam

import (
	"context"
	"testing"

	"lostluck.dev/beam-go/internal/runner/prism"
)

func TestOptions_Endpoint(t *testing.T) {
	ctx := context.Background()
	h, err := prism.Start(ctx, prism.Options{Port: "8074"})
	if err != nil {
		t.Skip()
	}
	defer h.Terminate()

	if _, err := LaunchAndWait(ctx, func(s *Scope) error {
		Impulse(s)
		return nil
	}, Endpoint(h.Addr())); err != nil {
		t.Errorf("pipeline failed to launch: %v", err)
	}
}
