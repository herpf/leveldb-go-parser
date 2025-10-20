package log

import (
	"bytes"
	"fmt"
	"hash/crc32"
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
	Recovered      bool
}

type WriteBatch struct {
	Offset         int64
	SequenceNumber uint64
	Count          uint32
	Records        []common.ParsedInternalKey
	Recovered      bool
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

	contentsOffset, contents, err := decoder.ReadBytes(int(length))
	if err != nil {
		return nil, err
	}

	isPartial := len(contents) < int(length)
	recovered := false

	// Validate checksum
	table := crc32.MakeTable(crc32.Castagnoli)
	computed := crc32.Checksum(append([]byte{recordTypeByte}, contents...), table)
	expected := unmask(checksum)

	if computed != expected {
		fmt.Fprintf(os.Stderr, "Checksum-Ignore-LOG: Ignoring mismatch at offset %d (computed %x, expected %x). Recovering.\n", baseOffset+offset, computed, expected)
		recovered = true
	}

	// Now check for trailer (after validation)
	if recordTypeByte == 0 && length == 0 {
		fmt.Fprintf(os.Stderr, "Debug-LOG: Found zero trailer at offset %d, likely end of block data.\n", baseOffset+offset)
		return nil, io.EOF
	}

	// Invalid zero type with non-zero length
	if recordTypeByte == 0 && length != 0 {
		fmt.Fprintf(os.Stderr, "Warning-LOG: Invalid zero type with non-zero length at offset %d. Skipping.\n", baseOffset+offset)
		return nil, fmt.Errorf("invalid zero type")
	}

	if isPartial {
		fmt.Fprintf(os.Stderr, "Partial-Recovery-LOG: Recovering partial physical record at offset %d (len %d/%d)\n", baseOffset+offset, len(contents), length)
		recovered = true
	}

	fmt.Fprintf(os.Stderr, "Debug-LOG: Decoded PhysicalRecord: Offset=%d, Type=%d, Length=%d (Recovered=%t)\n",
		baseOffset+offset, recordTypeByte, length, recovered)

	return &PhysicalRecord{
		BaseOffset:     baseOffset,
		Offset:         offset + baseOffset,
		Checksum:       checksum,
		Length:         length,
		RecordType:     recordTypeByte,
		Contents:       contents,
		ContentsOffset: contentsOffset + baseOffset,
		Recovered:      recovered,
	}, nil
}

func mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

func unmask(masked uint32) uint32 {
	crc := masked - 0xa282ead8
	return ((crc >> 17) | (crc << 15))
}

func decodeWriteBatch(data []byte, contentsBaseOffset int64) (*WriteBatch, error) {
	decoder := common.NewLevelDBDecoder(bytes.NewReader(data))
	batchStartOffset := contentsBaseOffset // This is now an absolute offset

	fmt.Fprintf(os.Stderr, "Debug-LOG: Attempting decodeWriteBatch for data at offset %d (length %d)\n", batchStartOffset, len(data))

	// Try to read header, but recover on error
	sequenceNumber := uint64(0)
	count := uint32(0)
	recovered := false

	fmt.Fprintf(os.Stderr, "Debug-LOG: > Reading SequenceNumber at offset %d\n", batchStartOffset+decoder.Offset())
	headerOffset, seq, err := decoder.DecodeUint64() // headerOffset is offset within 'data' (0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Debug-LOG: Error decoding WriteBatch header (seqnum) at offset %d: %v. Recovering with seq=0\n", batchStartOffset+decoder.Offset(), err)
		recovered = true
	} else {
		sequenceNumber = seq

		fmt.Fprintf(os.Stderr, "Debug-LOG: > Reading Count at offset %d\n", batchStartOffset+decoder.Offset())
		_, c, err := decoder.DecodeUint32()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Debug-LOG: Error decoding WriteBatch header (count) at offset %d: %v. Recovering with count=0\n", batchStartOffset+decoder.Offset(), err)
			recovered = true
		} else {
			count = c
			// batchStartOffset+headerOffset is the absolute offset of the sequence number
			fmt.Fprintf(os.Stderr, "Debug-LOG: WriteBatch Header OK: Seq=%d, Count=%d at header offset %d\n", sequenceNumber, count, batchStartOffset+headerOffset)
		}
	}

	var records []common.ParsedInternalKey
	for i := uint32(0); ; i++ {
		if count > 0 && i >= count {
			break
		}

		keyStartOffset := decoder.Offset()                       // This is the offset within 'data'
		absoluteKeyOffset := contentsBaseOffset + keyStartOffset // This is the absolute file offset

		fmt.Fprintf(os.Stderr, "Debug-LOG: >> Attempting key %d/%d at absolute offset %d (batch offset %d)\n", i+1, count, absoluteKeyOffset, keyStartOffset)

		_, recordType, err := decoder.DecodeUint8()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Partial-Recovery-LOG: Partial batch decode stopped at record %d/%d (error on type): %v\n", i+1, count, err)
			recovered = true
			break
		}

		fmt.Fprintf(os.Stderr, "Debug-LOG: >>> Reading key blob for key %d/%d at absolute offset %d\n", i+1, count, absoluteKeyOffset+1)
		_, key, err := decoder.DecodeBlobWithLength()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Partial-Recovery-LOG: Partial batch decode stopped at record %d/%d (error on key): %v\n", i+1, count, err)
			recovered = true
			break
		}

		var value []byte
		if recordType == TypeValue {
			fmt.Fprintf(os.Stderr, "Debug-LOG: >>>> Reading value blob for key %d/%d at absolute offset %d\n", i+1, count, contentsBaseOffset+decoder.Offset())
			_, value, err = decoder.DecodeBlobWithLength()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Partial-Recovery-LOG: Partial batch decode stopped at record %d/%d (error on value): %v\n", i+1, count, err)
				recovered = true
				break
			}
		}

		fmt.Fprintf(os.Stderr, "Debug-LOG: Decoded InternalKey %d/%d: AbsOffset=%d, Type=%d, KeyHex=%x\n", i+1, count, absoluteKeyOffset, recordType, key)

		newRecord := common.ParsedInternalKey{
			Offset:         absoluteKeyOffset, // This is now the absolute file offset
			RecordType:     recordType,
			Key:            key,
			Value:          value,
			SequenceNumber: sequenceNumber + uint64(i),
		}

		if newRecord.Offset == 264328 {
			fmt.Fprintf(os.Stderr, "!!!!!!!! TARGET RECORD FOUND (LOG) !!!!!!!! Offset: %d, KeyHex: %x, ValueLen: %d\n", newRecord.Offset, newRecord.Key, len(newRecord.Value))
		}

		records = append(records, newRecord)

	}

	if recovered {
		fmt.Fprintf(os.Stderr, "Debug-LOG: Successfully decoded partial WriteBatch starting at offset %d with %d records\n", batchStartOffset, len(records))
	} else {
		fmt.Fprintf(os.Stderr, "Debug-LOG: Successfully decoded WriteBatch starting at offset %d\n", batchStartOffset)
	}

	return &WriteBatch{
		Offset:         batchStartOffset + headerOffset, // Absolute offset of sequence number
		SequenceNumber: sequenceNumber,
		Count:          uint32(len(records)),
		Records:        records,
		Recovered:      recovered,
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
		for i := range batch.Records {
			batch.Records[i].Recovered = batch.Recovered // Set per-record
			allKeys = append(allKeys, batch.Records[i])
		}
	}
	return allKeys, nil
}

func (fr *FileReader) getWriteBatches() ([]*WriteBatch, error) {
	physicalRecords, err := fr.getPhysicalRecords()

	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL-LOG: getPhysicalRecords returned error for %s: %v. Aborting getWriteBatches.\n", fr.filename, err)
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "CRITICAL-LOG: getWriteBatches received %d physical records for %s.\n", len(physicalRecords), fr.filename)

	var batches []*WriteBatch
	var buffer []byte
	var firstRecordOffset int64 = -1

	fmt.Fprintf(os.Stderr, "MINIMAL-LOG: >>> ENTERING for loop over physicalRecords (len=%d) for %s\n", len(physicalRecords), fr.filename)

	for i, rec := range physicalRecords {

		// rec.ContentsOffset is now an absolute file offset
		fmt.Fprintf(os.Stderr, "MINIMAL-LOG: >>> INSIDE for loop, index %d, file %s\n", i, fr.filename)
		fmt.Fprintf(os.Stderr, "TYPE-CHECK-LOG: index %d, file %s, Offset: %d, RecordType: %d\n",
			i, fr.filename, rec.ContentsOffset, rec.RecordType)

		switch rec.RecordType {
		case TypeFull:
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Entered TypeFull case for index %d, file %s, offset %d\n", i, fr.filename, rec.ContentsOffset)

			if buffer != nil {
				fmt.Fprintf(os.Stderr, "Warning-LOG: Found TypeFull while previous buffer was not nil (incomplete batch?). Attempting recovery. Prev FirstOffset: %d, Current Offset: %d\n", firstRecordOffset, rec.ContentsOffset)
				recoveryBatch, recoveryErr := decodeWriteBatch(buffer, firstRecordOffset)
				if recoveryErr != nil {
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Failed to decode incomplete previous batch: %v\n", recoveryErr)
				} else {
					batches = append(batches, recoveryBatch)
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Successfully recovered incomplete previous batch with %d records.\n", recoveryBatch.Count)
				}
				// Reset after attempt
				buffer = nil
				firstRecordOffset = -1
			}

			batch, err := decodeWriteBatch(rec.Contents, rec.ContentsOffset)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning-LOG: failed to decode full write batch at offset %d, skipping: %v\n", rec.ContentsOffset, err)
				buffer = nil
				continue
			}
			if rec.Recovered {
				batch.Recovered = true
			}
			batches = append(batches, batch)
			buffer = nil
		case TypeFirst:
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Entered TypeFirst case for index %d, file %s, offset %d, content length %d\n", i, fr.filename, rec.ContentsOffset, len(rec.Contents))

			if buffer != nil {
				fmt.Fprintf(os.Stderr, "Warning-LOG: Found TypeFirst while previous buffer was not nil (incomplete batch?). Attempting recovery. Prev FirstOffset: %d, Current Offset: %d\n", firstRecordOffset, rec.ContentsOffset)
				recoveryBatch, recoveryErr := decodeWriteBatch(buffer, firstRecordOffset)
				if recoveryErr != nil {
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Failed to decode incomplete previous batch: %v\n", recoveryErr)
				} else {
					batches = append(batches, recoveryBatch)
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Successfully recovered incomplete previous batch with %d records.\n", recoveryBatch.Count)
				}
				buffer = nil
				firstRecordOffset = -1
			}

			buffer = make([]byte, len(rec.Contents))
			copy(buffer, rec.Contents)
			firstRecordOffset = rec.ContentsOffset
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Assigned buffer (len=%d) and firstRecordOffset=%d for index %d\n", len(buffer), firstRecordOffset, i)
			fmt.Fprintf(os.Stderr, "Debug-LOG: Started multi-part batch at offset %d\n", firstRecordOffset)
		case TypeMiddle:
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Entered TypeMiddle case for index %d, file %s, offset %d\n", i, fr.filename, rec.ContentsOffset)
			if buffer != nil {
				buffer = append(buffer, rec.Contents...)
				fmt.Fprintf(os.Stderr, "TRACE-LOG: Appended %d bytes to buffer for TypeMiddle. New buffer len: %d\n", len(rec.Contents), len(buffer))
			} else {
				fmt.Fprintf(os.Stderr, "Warning-LOG: Found middle block without a preceding first block near offset %d\n", rec.ContentsOffset)
			}
		case TypeLast:
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Entered TypeLast case for index %d, file %s, offset %d\n", i, fr.filename, rec.ContentsOffset)
			if buffer != nil {
				buffer = append(buffer, rec.Contents...)
				fmt.Fprintf(os.Stderr, "TRACE-LOG: Appended %d bytes to buffer for TypeLast. Final buffer len: %d. Attempting decode.\n", len(rec.Contents), len(buffer))
				batch, err := decodeWriteBatch(buffer, firstRecordOffset)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning-LOG: failed to decode multi-part write batch starting near offset %d, skipping: %v\n", firstRecordOffset, err)
					buffer = nil
					firstRecordOffset = -1
					continue
				}
				if rec.Recovered {
					batch.Recovered = true
				}
				batches = append(batches, batch)
				fmt.Fprintf(os.Stderr, "Debug-LOG: Completed multi-part batch starting near offset %d.\n", firstRecordOffset)
				buffer = nil
				firstRecordOffset = -1
			} else {
				fmt.Fprintf(os.Stderr, "Warning-LOG: Found last block without preceding blocks near offset %d\n", rec.ContentsOffset)
			}
		default:
			fmt.Fprintf(os.Stderr, "UNEXPECTED-TYPE-LOG: index %d, file %s, Offset: %d, Encountered unexpected RecordType: %d\n",
				i, fr.filename, rec.ContentsOffset, rec.RecordType)

			if buffer != nil {
				fmt.Fprintf(os.Stderr, "Warning-LOG: Unexpected type while previous buffer was not nil. Attempting recovery. Prev FirstOffset: %d, Current Offset: %d\n", firstRecordOffset, rec.ContentsOffset)
				recoveryBatch, recoveryErr := decodeWriteBatch(buffer, firstRecordOffset)
				if recoveryErr != nil {
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Failed to decode incomplete previous batch: %v\n", recoveryErr)
				} else {
					batches = append(batches, recoveryBatch)
					fmt.Fprintf(os.Stderr, "Recovery-LOG: Successfully recovered incomplete previous batch with %d records.\n", recoveryBatch.Count)
				}
				buffer = nil
				firstRecordOffset = -1
			}
		}
		fmt.Fprintf(os.Stderr, "STATE-LOG: After index %d, file %s, offset %d, type %d: buffer len=%d, firstOffset=%d\n", i, fr.filename, rec.ContentsOffset, rec.RecordType, len(buffer), firstRecordOffset)
	}

	fmt.Fprintf(os.Stderr, "MINIMAL-LOG: <<< EXITED for loop over physicalRecords for %s\n", fr.filename)

	if buffer != nil {
		fmt.Fprintf(os.Stderr, "Warning-LOG: Log file %s ended with an incomplete multi-part write batch starting near offset %d\n", fr.filename, firstRecordOffset)
		fmt.Fprintf(os.Stderr, "Recovery-LOG: Attempting to decode incomplete batch at offset %d (length %d)\n", firstRecordOffset, len(buffer))
		batch, err := decodeWriteBatch(buffer, firstRecordOffset)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Recovery-LOG: Failed to decode incomplete batch: %v\n", err)
		} else {
			batches = append(batches, batch)
			fmt.Fprintf(os.Stderr, "Recovery-LOG: Successfully recovered and decoded incomplete batch with %d records.\n", batch.Count)
		}
	}
	return batches, nil
}

func (fr *FileReader) getPhysicalRecords() ([]*PhysicalRecord, error) {
	f, err := os.Open(fr.filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL-LOG: Failed to open file %s: %v\n", fr.filename, err)
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL-LOG: Failed to stat file %s: %v\n", fr.filename, err)
		return nil, fmt.Errorf("could not stat file %s: %w", fr.filename, err)
	}
	fileSize := stat.Size()

	var records []*PhysicalRecord
	var blockOffset int64 = 0

	fmt.Fprintf(os.Stderr, "TRACE-LOG: Starting getPhysicalRecords for %s (Size: %d)\n", fr.filename, fileSize)

	for blockOffset < fileSize {
		fmt.Fprintf(os.Stderr, "Block-Start-LOG: Starting block at offset %d, fileSize %d\n", blockOffset, fileSize)

		bytesToRead := int64(BlockSize)
		if blockOffset+bytesToRead > fileSize {
			bytesToRead = fileSize - blockOffset
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Adjusting bytesToRead for last block: %d\n", bytesToRead)
		}

		blockData := make([]byte, bytesToRead)

		_, seekErr := f.Seek(blockOffset, io.SeekStart)
		if seekErr != nil {
			fmt.Fprintf(os.Stderr, "CRITICAL-LOG: Seek failed at offset %d: %v. Stopping file processing.\n", blockOffset, seekErr)
			return records, fmt.Errorf("seek error at offset %d: %w", blockOffset, seekErr)
		}

		n, readErr := io.ReadFull(f, blockData)
		fmt.Fprintf(os.Stderr, "ReadFull-LOG: Read n=%d, readErr=%v at offset %d\n", n, readErr, blockOffset)

		if readErr != nil {
			if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
				fmt.Fprintf(os.Stderr, "TRACE-LOG: ReadFull returned %v after n=%d at offset %d. Treating as end.\n", readErr, n, blockOffset)
				blockData = blockData[:n]
			} else {
				fmt.Fprintf(os.Stderr, "CRITICAL-LOG: ReadFull failed with unexpected error at offset %d: %v. Stopping file processing.\n", blockOffset, readErr)
				return records, fmt.Errorf("file read error at offset %d: %w", blockOffset, readErr)
			}
		}

		if n == 0 {
			fmt.Fprintf(os.Stderr, "Empty-Read-LOG: Read n=0 at offset %d with readErr=%v. Treating as empty block.\n", blockOffset, readErr)
		}

		if n > 0 {
			fmt.Fprintf(os.Stderr, "Processing-Block-LOG: Processing block at %d with n %d\n", blockOffset, n)

			decoder := common.NewLevelDBDecoder(bytes.NewReader(blockData[:n]))
			blockRecordCount := 0
			for {
				record, decodeErr := decodePhysicalRecord(decoder, blockOffset)
				if decodeErr != nil {
					if decodeErr == io.EOF || decodeErr == io.ErrUnexpectedEOF {
						fmt.Fprintf(os.Stderr, "End-of-Block-LOG: decodePhysicalRecord hit end in block at offset ~%d.\n", blockOffset+decoder.Offset())
					} else {
						fmt.Fprintf(os.Stderr, "ERROR-LOG: decodePhysicalRecord failed in block starting %d at offset ~%d: %v. Stopping processing for THIS BLOCK.\n", blockOffset, blockOffset+decoder.Offset(), decodeErr)
					}
					break
				}
				records = append(records, record)
				blockRecordCount++
			}
			fmt.Fprintf(os.Stderr, "TRACE-LOG: Finished processing block starting %d. Decoded %d records.\n", blockOffset, blockRecordCount)
		}

		// Always advance by bytesToRead, even if n == 0
		blockOffset += bytesToRead
		fmt.Fprintf(os.Stderr, "Advance-LOG: Advanced blockOffset to %d\n", blockOffset)
	}

	fmt.Fprintf(os.Stderr, "TRACE-LOG: Exited getPhysicalRecords loop for %s. Total records: %d\n", fr.filename, len(records))
	return records, nil
}
