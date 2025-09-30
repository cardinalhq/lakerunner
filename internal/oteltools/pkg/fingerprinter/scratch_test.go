// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package fingerprinter

//
//import (
//	"fmt"
//	"testing"
//)
//
//func TestRandom(t *testing.T) {
//	fp := NewFingerprinter()
//	input := "[c977a1b7-99d5-4419-af37-cada1836b1d0]   \\u001b[1m\\u001b[36mTicket Load (1.8ms)\\u001b[0m  \\u001b[1m\\u001b[34mSELECT `tickets`.* FROM `tickets` WHERE `tickets`.`account_id` = 11 AND `tickets`.`id` IN (106624, 106625, 106509, 106545, 106603, 106486, 106560, 106605, 106562, 107137, 107139, 107013, 107063, 107077, 107086, 107068, 107011, 107025, 107007, 107618, 106975, 106954, 107481, 107453, 107368, 107579, 107584, 106953, 107327, 107314)\\u001b[0m"
//	tokenizeInput, _, err := fp.Tokenize(input)
//	if err != nil {
//		return
//	}
//	if len(tokenizeInput) == 0 {
//		return
//	}
//	for index, token := range tokenizeInput {
//		println(fmt.Sprintf("%d.%s", index, token))
//	}
//
//}
