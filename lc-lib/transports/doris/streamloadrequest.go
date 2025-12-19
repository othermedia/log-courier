/*
 * Copyright 2012-2020 Jason Woods and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package doris

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/driskell/log-courier/lc-lib/event"
)

type streamLoadRequestCursor struct {
	pos   []*event.Event
	moved bool
}

type streamLoadRequest struct {
	// Constructor
	events         []*event.Event
	markCursor     *streamLoadRequestCursor
	readCursor     *streamLoadRequestCursor
	created        int
	remaining      int
	ackSequence    uint32
	columnNames    []string
	restJSONColumn string

	// Internal
	currentBytes []byte
}

func newStreamLoadRequest(columnNames []string, restJSONColumn string, events []*event.Event) *streamLoadRequest {
	eventsClone := append(events[:0:0], events...)

	return &streamLoadRequest{
		// events will be mutated and nil holes punched for successful events
		// and anything non-nil remains outstanding
		events:         eventsClone,
		markCursor:     &streamLoadRequestCursor{pos: eventsClone},
		readCursor:     &streamLoadRequestCursor{pos: eventsClone},
		created:        0,
		remaining:      len(events),
		ackSequence:    0,
		columnNames:    columnNames,
		restJSONColumn: restJSONColumn,
	}
}

// Created returns the number of events that have been successfully created
func (p *streamLoadRequest) Created() int {
	return p.created
}

// Remaining returns the number of events left to send in this request
func (p *streamLoadRequest) Remaining() int {
	return p.remaining
}

// AckSequence returns the sequence marking the end of the contiguous events that we can ack
func (p *streamLoadRequest) AckSequence() uint32 {
	return p.ackSequence
}

// Event returns the event data at the given cursor position, use nil for the beginning
func (p *streamLoadRequest) Event(cursor *streamLoadRequestCursor) map[string]interface{} {
	if cursor == nil {
		return p.markCursor.pos[0].Data()
	}
	return cursor.pos[0].Data()
}

// Mark sets the status of the first outstanding event based on the given successful value
// It returns a cursor which can then be passed in to mark the next item, and repeat
// Pass in nil cursor to start from the beginning
// Returns true when the cursor reaches the end
func (p *streamLoadRequest) Mark(cursor *streamLoadRequestCursor, successful bool) (*streamLoadRequestCursor, bool) {
	var currentCursor *streamLoadRequestCursor
	if cursor == nil {
		currentCursor = &streamLoadRequestCursor{pos: p.markCursor.pos}
	} else {
		currentCursor = cursor
	}

	if !successful {
		currentCursor.pos = currentCursor.pos[1:]
		currentCursor.moved = true
		if len(currentCursor.pos) == 0 {
			return nil, true
		}
	} else {
		currentCursor.pos[0] = nil
		p.remaining--
		p.created++
	}

	for currentCursor.pos[0] == nil {
		currentCursor.pos = currentCursor.pos[1:]
		if !currentCursor.moved {
			p.ackSequence++
		}
		if len(currentCursor.pos) == 0 {
			break
		}
	}

	if !currentCursor.moved {
		p.markCursor.pos = currentCursor.pos
	}

	if len(currentCursor.pos) == 0 {
		return nil, true
	}
	return currentCursor, false
}

// Reset allows the request to be Read again
func (p *streamLoadRequest) Reset() {
	p.readCursor.pos = p.markCursor.pos
}

// Read implements io.Reader and returns a JSON array of events for Doris stream load
func (p *streamLoadRequest) Read(dst []byte) (n int, err error) {
	for len(dst) > 0 {
		if p.currentBytes == nil {
			if len(p.readCursor.pos) == 0 {
				return n, io.EOF
			}

			// Convert event to JSON object with mapped columns
			eventData := p.readCursor.pos[0].Data()
			mappedEvent := make(map[string]interface{})
			restData := make(map[string]interface{})

			// Map known columns
			for _, colName := range p.columnNames {
				if colName == p.restJSONColumn {
					continue
				}

				if value, ok := eventData[colName]; ok {
					mappedEvent[colName] = p.formatValue(colName, value)
				} else {
					mappedEvent[colName] = nil
				}
			}

			// Collect unmapped fields into rest JSON column
			for key, value := range eventData {
				if !p.isColumnMapped(key) && key != "@timestamp" {
					// Include all unmapped fields, including metadata
					restData[key] = value
				}
			}

			// Add rest JSON column if there's data
			if len(restData) > 0 {
				mappedEvent[p.restJSONColumn] = restData
			} else {
				mappedEvent[p.restJSONColumn] = nil
			}

			jsonBytes, err := json.Marshal(mappedEvent)
			if err != nil {
				return n, fmt.Errorf("failed to marshal event: %s", err)
			}

			p.currentBytes = append(jsonBytes, '\n')

			p.readCursor.pos = p.readCursor.pos[1:]
			for len(p.readCursor.pos) != 0 && p.readCursor.pos[0] == nil {
				p.readCursor.pos = p.readCursor.pos[1:]
			}
		}

		copied := copy(dst, p.currentBytes)
		if copied >= len(p.currentBytes) {
			p.currentBytes = nil
		} else {
			p.currentBytes = p.currentBytes[copied:]
		}

		n += copied
		dst = dst[copied:]
	}

	return
}

// formatValue formats a value for Doris based on the column type
func (p *streamLoadRequest) formatValue(colName string, value interface{}) interface{} {
	if colName == "@timestamp" {
		switch v := value.(type) {
		case event.Timestamp:
			return time.Time(v).Format("2006-01-02 15:04:05")
		case time.Time:
			return v.Format("2006-01-02 15:04:05")
		case string:
			return v
		}
	}

	if colName == "tags" {
		switch v := value.(type) {
		case event.Tags:
			return []string(v)
		case []string:
			return v
		case []interface{}:
			tags := make([]string, 0, len(v))
			for _, tag := range v {
				if str, ok := tag.(string); ok {
					tags = append(tags, str)
				}
			}
			return tags
		}
	}

	// For other types, return as-is
	return value
}

// isColumnMapped checks if a field is mapped to a column
func (p *streamLoadRequest) isColumnMapped(field string) bool {
	for _, col := range p.columnNames {
		if col == field {
			return true
		}
	}
	return false
}
