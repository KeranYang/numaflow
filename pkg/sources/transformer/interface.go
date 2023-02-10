package transformer

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Transformer interface {
	Transform(context.Context, []*isb.ReadMessage) []*isb.ReadMessage
}
