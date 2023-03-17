// VulcanizeDB
// Copyright © 2021 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package statediff

import (
	"encoding/json"
	"math/big"
)

// Payload packages the data to send to statediff subscriptions
type Payload struct {
	BlockRlp        []byte   `json:"blockRlp"`
	TotalDifficulty *big.Int `json:"totalDifficulty"`
	ReceiptsRlp     []byte   `json:"receiptsRlp"`
	StateObjectRlp  []byte   `json:"stateObjectRlp"    gencodec:"required"`

	encoded []byte
	err     error
}

func (sd *Payload) ensureEncoded() {
	if sd.encoded == nil && sd.err == nil {
		sd.encoded, sd.err = json.Marshal(sd)
	}
}

// Length to implement Encoder interface for Payload
func (sd *Payload) Length() int {
	sd.ensureEncoded()
	return len(sd.encoded)
}

// Encode to implement Encoder interface for Payload
func (sd *Payload) Encode() ([]byte, error) {
	sd.ensureEncoded()
	return sd.encoded, sd.err
}

// Subscription struct holds our subscription channels
type Subscription struct {
	PayloadChan chan<- Payload
	QuitChan    chan<- bool
}
