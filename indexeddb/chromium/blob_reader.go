// File: chromium/blob_reader.go
package chromium

import (
	"fmt"
	"os"
	"path/filepath"
)

// BlobFolderReader handles reading external blobs from the .blob directory.
type BlobFolderReader struct {
	folderPath string
}

// NewBlobFolderReader initializes a reader for the given .blob folder.
func NewBlobFolderReader(folderName string) (*BlobFolderReader, error) {
	info, err := os.Stat(folderName)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("blob folder %s is not a valid directory", folderName)
	}

	absPath, err := filepath.Abs(folderName)
	if err != nil {
		return nil, err
	}

	return &BlobFolderReader{folderPath: absPath}, nil
}

// ReadBlob locates and reads the physical file for a given database and blob ID.
func (r *BlobFolderReader) ReadBlob(databaseID int, blobID uint64) (string, []byte, error) {
	// 1. Database directory: hex representation of the DB ID
	dbDir := fmt.Sprintf("%x", databaseID)

	// 2. Sub-folder: the second byte of the blob ID, zero-padded to 2 hex characters
	blobSubDir := fmt.Sprintf("%02x", (blobID&0xff00)>>8)

	// 3. Blob file name: hex representation of the blob ID
	blobFile := fmt.Sprintf("%x", blobID)

	// Construct the full path
	blobPath := filepath.Join(r.folderPath, dbDir, blobSubDir, blobFile)

	// Read the file from the OS
	data, err := os.ReadFile(blobPath)
	if err != nil {
		return blobPath, nil, fmt.Errorf("failed to read blob file at %s: %w", blobPath, err)
	}

	return blobPath, data, nil
}
