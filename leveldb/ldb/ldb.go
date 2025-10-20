package ldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"leveldb-parser-go/leveldb/common"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// Constants from LevelDB format specification
const (
	BlockTrailerSize            = 5
	PackedSequenceAndTypeLength = 8
	BlockRestartEntryLength     = 4
	TableFooterSize             = 48
)

var tableMagic = []byte{0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb}

// BlockCompressionType constants
const (
	NoCompression byte = 0x0
	Snappy        byte = 0x1
	Zstd          byte = 0x4
)

type Block struct {
	Offset      int64
	BlockOffset int64
	Data        []byte
	Footer      []byte
}

func (b *Block) GetBuffer() ([]byte, error) {
	if len(b.Footer) == 0 {
		return b.Data, nil // Assume no compression if footer is missing
	}
	compressionType := b.Footer[0]
	switch compressionType {
	case Snappy:
		return snappy.Decode(nil, b.Data)
	case Zstd:
		reader, err := zstd.NewReader(bytes.NewReader(b.Data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	default:
		return b.Data, nil
	}
}

func (b *Block) GetRecords() ([]common.KeyValueRecord, error) {
	buffer, err := b.GetBuffer()
	if err != nil {
		return nil, fmt.Errorf("failed to get block buffer: %w", err)
	}

	if len(buffer) < BlockRestartEntryLength {
		return []common.KeyValueRecord{}, nil
	}

	numRestartsOffset := len(buffer) - BlockRestartEntryLength
	numRestarts := binary.LittleEndian.Uint32(buffer[numRestartsOffset:])

	trailerSize := (int(numRestarts) + 1) * BlockRestartEntryLength
	if trailerSize > len(buffer) {
		fmt.Fprintf(os.Stderr, "Warning: block at offset %d has corrupt trailer. Num restarts (%d) is too large for buffer size (%d).\n", b.BlockOffset, numRestarts, len(buffer))
		return []common.KeyValueRecord{}, nil
	}

	restartsOffset := len(buffer) - trailerSize
	// CORRECTED: bytes.NewReader implements io.ReadSeeker, which our new decoder needs.
	decoder := common.NewLevelDBDecoder(bytes.NewReader(buffer[:restartsOffset]))
	var records []common.KeyValueRecord
	var sharedKey []byte

	for {
		record, newSharedKey, err := decodeKeyValueRecord(decoder, b.BlockOffset, sharedKey)
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Warning: Corrupt record in LDB block at offset %d: %v. Attempting to recover.\n", b.BlockOffset+decoder.Offset(), err)
			sharedKey = []byte{}
			return nil, fmt.Errorf("failed to decode key-value record: %w", err)
		}
		records = append(records, record)
		sharedKey = newSharedKey
	}
	return records, nil
}

func decodeKeyValueRecord(decoder *common.LevelDBDecoder, blockOffset int64, sharedKey []byte) (common.KeyValueRecord, []byte, error) {
	// CORRECTED: Replaced DecodeUint32Varint with the new DecodeVarint
	offset, sharedBytes, err := decoder.DecodeVarint()
	if err != nil {
		return common.KeyValueRecord{}, nil, err
	}
	_, unsharedBytes, err := decoder.DecodeVarint()
	if err != nil {
		return common.KeyValueRecord{}, nil, err
	}
	_, valueLength, err := decoder.DecodeVarint()
	if err != nil {
		return common.KeyValueRecord{}, nil, err
	}
	_, keyDelta, err := decoder.ReadBytes(int(unsharedBytes))
	if err != nil {
		return common.KeyValueRecord{}, nil, err
	}
	_, value, err := decoder.ReadBytes(int(valueLength))
	if err != nil {
		return common.KeyValueRecord{}, nil, err
	}

	newSharedKey := make([]byte, sharedBytes)
	copy(newSharedKey, sharedKey[:sharedBytes])
	newSharedKey = append(newSharedKey, keyDelta...)

	if len(newSharedKey) < PackedSequenceAndTypeLength {
		return common.KeyValueRecord{}, nil, fmt.Errorf("invalid shared key length")
	}

	key := newSharedKey[:len(newSharedKey)-PackedSequenceAndTypeLength]
	packedFooter := newSharedKey[len(newSharedKey)-PackedSequenceAndTypeLength:]
	sequenceAndType := binary.LittleEndian.Uint64(packedFooter)
	sequenceNumber := sequenceAndType >> 8
	keyType := byte(sequenceAndType & 0xff)

	return common.KeyValueRecord{
		Offset:         int64(offset) + blockOffset, // Cast offset to int64
		Key:            key,
		Value:          value,
		SequenceNumber: sequenceNumber,
		RecordType:     keyType,
	}, newSharedKey, nil
}

type BlockHandle struct {
	Offset      int64
	BlockOffset int64
	Length      int
}

func decodeBlockHandle(decoder *common.LevelDBDecoder, baseOffset int64) (BlockHandle, error) {
	// CORRECTED: Replaced DecodeUint64Varint with the new DecodeVarint
	offset, blockOffset, err := decoder.DecodeVarint()
	if err != nil {
		return BlockHandle{}, err
	}
	_, length, err := decoder.DecodeVarint()
	if err != nil {
		return BlockHandle{}, err
	}
	return BlockHandle{
		Offset:      int64(offset) + baseOffset,
		BlockOffset: int64(blockOffset),
		Length:      int(length),
	}, nil
}

func (bh *BlockHandle) Load(f *os.File) (Block, error) {
	data := make([]byte, bh.Length)
	_, err := f.ReadAt(data, bh.BlockOffset)
	if err != nil {
		return Block{}, fmt.Errorf("could not read block data: %w", err)
	}

	footer := make([]byte, BlockTrailerSize)
	_, err = f.ReadAt(footer, bh.BlockOffset+int64(bh.Length))
	if err != nil && err != io.EOF {
		return Block{}, fmt.Errorf("could not read block footer: %w", err)
	}
	return Block{
		Offset:      bh.Offset,
		BlockOffset: bh.BlockOffset,
		Data:        data,
		Footer:      footer,
	}, nil
}

type FileReader struct {
	filename   string
	indexBlock Block
}

func NewFileReader(filename string) (*FileReader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < TableFooterSize {
		return nil, fmt.Errorf("file too small for footer: %s", filename)
	}

	footerBuf := make([]byte, len(tableMagic))
	_, err = f.ReadAt(footerBuf, fileSize-int64(len(tableMagic)))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(footerBuf, tableMagic) {
		return nil, fmt.Errorf("invalid magic number in %s", filename)
	}

	// CORRECTED: *os.File implements io.ReadSeeker, so this works with the new decoder
	footerDecoder := common.NewLevelDBDecoder(f)
	footerDecoder.Seek(fileSize-TableFooterSize, io.SeekStart)

	_, err = decodeBlockHandle(footerDecoder, 0) // Skip metaindex handle
	if err != nil {
		return nil, fmt.Errorf("failed to decode metaindex handle: %w", err)
	}
	indexHandle, err := decodeBlockHandle(footerDecoder, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index handle: %w", err)
	}
	indexBlock, err := indexHandle.Load(f)
	if err != nil {
		return nil, fmt.Errorf("failed to load index block: %w", err)
	}

	return &FileReader{filename: filename, indexBlock: indexBlock}, nil
}

func (fr *FileReader) GetKeyValueRecords() ([]common.KeyValueRecord, error) {
	var allRecords []common.KeyValueRecord
	f, err := os.Open(fr.filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	indexRecords, err := fr.indexBlock.GetRecords()
	if err != nil {
		return nil, fmt.Errorf("failed to get records from index block: %w", err)
	}

	for _, indexRecord := range indexRecords {
		// CORRECTED: bytes.NewReader implements io.ReadSeeker
		handleDecoder := common.NewLevelDBDecoder(bytes.NewReader(indexRecord.Value))
		blockHandle, err := decodeBlockHandle(handleDecoder, indexRecord.Offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode block handle: %w", err)
			// fmt.Fprintf(os.Stderr, "Warning: failed to decode block handle: %v\n", err)
		}
		dataBlock, err := blockHandle.Load(f)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
			// fmt.Fprintf(os.Stderr, "Warning: failed to load data block: %v\n", err)
		}
		dataRecords, err := dataBlock.GetRecords()
		if err != nil {
			return nil, fmt.Errorf("failed to get records from data block: %w", err)
			// fmt.Fprintf(os.Stderr, "Warning: failed to get records from data block: %v\n", err)
		}
		allRecords = append(allRecords, dataRecords...)
	}
	return allRecords, nil
}
