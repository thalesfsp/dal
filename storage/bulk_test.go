package storage

import (
	"errors"
	"testing"
)

func TestChunkIndexedPreservesOriginalIndices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		itemCount  int
		wantChunks []int
	}{
		{name: "smaller than cap", itemCount: 24, wantChunks: []int{24}},
		{name: "exactly cap", itemCount: 25, wantChunks: []int{25}},
		{name: "one over cap", itemCount: 26, wantChunks: []int{25, 1}},
		{name: "many multiples", itemCount: 100, wantChunks: []int{25, 25, 25, 25}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items := make([]string, tt.itemCount)
			for i := range items {
				items[i] = string(rune(i))
			}

			chunks, err := ChunkIndexed(items, 25)
			if err != nil {
				t.Fatalf("ChunkIndexed() error = %v", err)
			}
			if len(chunks) != len(tt.wantChunks) {
				t.Fatalf("chunk count = %d, want %d", len(chunks), len(tt.wantChunks))
			}

			originalIndex := 0
			for chunkIndex, chunk := range chunks {
				if len(chunk) != tt.wantChunks[chunkIndex] {
					t.Fatalf("chunk %d length = %d, want %d", chunkIndex, len(chunk), tt.wantChunks[chunkIndex])
				}

				for _, indexed := range chunk {
					if indexed.Index != originalIndex {
						t.Fatalf("original index = %d, want %d", indexed.Index, originalIndex)
					}
					if indexed.Item != items[originalIndex] {
						t.Fatalf("item at original index %d = %q, want %q", originalIndex, indexed.Item, items[originalIndex])
					}

					originalIndex++
				}
			}
		})
	}
}

func TestChunkIndexedRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	chunks, err := ChunkIndexed([]int{1}, 0)
	if !errors.Is(err, ErrInvalidChunkLimit) {
		t.Fatalf("ChunkIndexed() error = %v, want %v", err, ErrInvalidChunkLimit)
	}
	if chunks != nil {
		t.Fatalf("ChunkIndexed() chunks = %v, want nil", chunks)
	}
}

func TestChunkIndexedEmptyInput(t *testing.T) {
	t.Parallel()

	chunks, err := ChunkIndexed([]int(nil), 25)
	if err != nil {
		t.Fatalf("ChunkIndexed() error = %v", err)
	}
	if chunks != nil {
		t.Fatalf("ChunkIndexed() chunks = %v, want nil", chunks)
	}
}
