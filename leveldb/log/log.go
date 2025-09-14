package log

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"leveldb-parser-go/leveldb/common"
)

// Constants from LevelDB format specification
const (
	BlockSize            = 32768
	PhysicalHeaderLength = 7
)

// LogFilePhysicalRecordType constants
const (
	TypeFull   byte = 1
	TypeFirst  byte = 2
	TypeMiddle byte = 3
	TypeLast   byte = 4
)

// InternalRecordType constants
const (
	TypeDeletion byte = 0x00
	TypeValue    byte = 0x01
)

type PhysicalRecord struct {
	BaseOffset     int64
	Offset         int64
	Checksum       uint32
	Length         uint16
	RecordType     byte
	Contents       []byte
	ContentsOffset int64
}

type WriteBatch struct {
	Offset         int64
	SequenceNumber uint64
	Count          uint32
	Records        []common.ParsedInternalKey
}

func decodePhysicalRecord(decoder *common.LevelDBDecoder, baseOffset int64) (*PhysicalRecord, error) {
	offset, checksum, err := decoder.DecodeUint32()
	if err != nil {
		return nil, err
	}
	_, length, err := decoder.DecodeUint16()
	if err != nil {
		return nil, err
	}
	_, recordTypeByte, err := decoder.DecodeUint8()
	if err != nil {
		return nil, err
	}

	if checksum == 0 && length == 0 && recordTypeByte == 0 {
		return nil, io.EOF // End of useful data in block
	}

	contentsOffset, contents, err := decoder.ReadBytes(int(length))
	if err != nil {
		return nil, err
	}
	// Check if the content was fully read
	if len(contents) < int(length) {
		return nil, io.ErrUnexpectedEOF
	}

	return &PhysicalRecord{
		BaseOffset:     baseOffset,
		Offset:         offset,
		Checksum:       checksum,
		Length:         length,
		RecordType:     recordTypeByte,
		Contents:       contents,
		ContentsOffset: contentsOffset,
	}, nil
}

func decodeWriteBatch(data []byte, baseOffset int64) (*WriteBatch, error) {
	decoder := common.NewLevelDBDecoder(bytes.NewReader(data))
	offset, sequenceNumber, err := decoder.DecodeUint64()
	if err != nil {
		return nil, err
	}
	_, count, err := decoder.DecodeUint32()
	if err != nil {
		return nil, err
	}

	var records []common.ParsedInternalKey
	for i := uint32(0); i < count; i++ {
		keyOffset, recordType, err := decoder.DecodeUint8()
		if err != nil {
			return nil, err
		}
		_, key, err := decoder.DecodeBlobWithLength()
		if err != nil {
			return nil, err
		}
		var value []byte
		if recordType == TypeValue {
			_, value, err = decoder.DecodeBlobWithLength()
			if err != nil {
				return nil, err
			}
		}

		records = append(records, common.ParsedInternalKey{
			Offset:         baseOffset + keyOffset,
			RecordType:     recordType,
			Key:            key,
			Value:          value,
			SequenceNumber: sequenceNumber + uint64(i),
		})
	}

	return &WriteBatch{
		Offset:         baseOffset + offset,
		SequenceNumber: sequenceNumber,
		Count:          count,
		Records:        records,
	}, nil
}

type FileReader struct {
	filename string
}

func NewFileReader(filename string) *FileReader {
	return &FileReader{filename: filename}
}

func (fr *FileReader) GetParsedInternalKeys() ([]common.ParsedInternalKey, error) {
	batches, err := fr.getWriteBatches()
	if err != nil {
		return nil, err
	}
	var allKeys []common.ParsedInternalKey
	for _, batch := range batches {
		allKeys = append(allKeys, batch.Records...)
	}
	return allKeys, nil
}

func (fr *FileReader) getWriteBatches() ([]*WriteBatch, error) {
	physicalRecords, err := fr.getPhysicalRecords()
	if err != nil {
		return nil, err
	}

	var batches []*WriteBatch
	var buffer []byte
	var batchOffset int64 = -1

	for _, rec := range physicalRecords {
		switch rec.RecordType {
		case TypeFull:
			batch, err := decodeWriteBatch(rec.Contents, rec.ContentsOffset+rec.BaseOffset)
			if err != nil {
				// Don't let a single corrupt batch stop the whole process
				fmt.Fprintf(os.Stderr, "Warning: failed to decode write batch, skipping: %v\n", err)
				continue
			}
			batches = append(batches, batch)
			buffer = nil
		case TypeFirst:
			buffer = make([]byte, len(rec.Contents))
			copy(buffer, rec.Contents)
			batchOffset = rec.ContentsOffset + rec.BaseOffset
		case TypeMiddle:
			if buffer != nil {
				buffer = append(buffer, rec.Contents...)
			}
		case TypeLast:
			if buffer != nil {
				buffer = append(buffer, rec.Contents...)
				batch, err := decodeWriteBatch(buffer, batchOffset)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: failed to decode write batch, skipping: %v\n", err)
					buffer = nil // Reset buffer on error
					continue
				}
				batches = append(batches, batch)
				buffer = nil
			}
		}
	}
	return batches, nil
}

func (fr *FileReader) getPhysicalRecords() ([]*PhysicalRecord, error) {
	f, err := os.Open(fr.filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []*PhysicalRecord
	var blockOffset int64 = 0

	for {
		blockData := make([]byte, BlockSize)
		n, err := f.Read(blockData)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		blockData = blockData[:n]
		decoder := common.NewLevelDBDecoder(bytes.NewReader(blockData))
		for {
			record, err := decodePhysicalRecord(decoder, blockOffset)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break // Reached end of parsable records in this block
				}
				// A real parsing error occurred
				return nil, err
			}
			records = append(records, record)
		}
		blockOffset += int64(n)
	}
	return records, nil
}
