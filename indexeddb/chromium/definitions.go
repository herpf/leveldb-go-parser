package chromium

// KeyPrefixType defines the type of data stored in the key.
type KeyPrefixType int

const (
	GlobalMetadata   KeyPrefixType = 0
	DatabaseMetadata KeyPrefixType = 1
	ObjectStoreData  KeyPrefixType = 2
	ExistsEntry      KeyPrefixType = 3
	IndexData        KeyPrefixType = 4
	InvalidType      KeyPrefixType = 5
	BlobEntry        KeyPrefixType = 6
)

type V8SerializationTag byte

const (
	V8VersionTag           V8SerializationTag = 0xFF
	V8Padding              V8SerializationTag = '\000'
	V8VerifyObjectCount    V8SerializationTag = '?'
	V8TheHole              V8SerializationTag = '-'
	V8Undefined            V8SerializationTag = '_'
	V8Null                 V8SerializationTag = '0'
	V8True                 V8SerializationTag = 'T'
	V8False                V8SerializationTag = 'F'
	V8Int32                V8SerializationTag = 'I'
	V8Uint32               V8SerializationTag = 'U'
	V8Double               V8SerializationTag = 'N'
	V8BigInt               V8SerializationTag = 'Z'
	V8UTF8String           V8SerializationTag = 'S'
	V8OneByteString        V8SerializationTag = '"'
	V8TwoByteString        V8SerializationTag = 'c'
	V8ObjectReference      V8SerializationTag = '^'
	V8BeginJSObject        V8SerializationTag = 'o'
	V8EndJSObject          V8SerializationTag = '{'
	V8BeginSparseJSArray   V8SerializationTag = 'a'
	V8EndSparseJSArray     V8SerializationTag = '@'
	V8BeginDenseJSArray    V8SerializationTag = 'A'
	V8EndDenseJSArray      V8SerializationTag = '$'
	V8Date                 V8SerializationTag = 'D'
	V8TrueObject           V8SerializationTag = 'y'
	V8FalseObject          V8SerializationTag = 'x'
	V8NumberObject         V8SerializationTag = 'n'
	V8BigIntObject         V8SerializationTag = 'z'
	V8StringObject         V8SerializationTag = 's'
	V8RegExp               V8SerializationTag = 'R'
	V8BeginJSMap           V8SerializationTag = ';'
	V8EndJSMap             V8SerializationTag = ':'
	V8BeginJSSet           V8SerializationTag = '\''
	V8EndJSSet             V8SerializationTag = ','
	V8ArrayBuffer          V8SerializationTag = 'B'
	V8ArrayBufferViewTag   V8SerializationTag = 'V'
	V8ResizableArrayBuffer V8SerializationTag = '~'
	V8ArrayBufferTransfer  V8SerializationTag = 't'
	V8SharedArrayBuffer    V8SerializationTag = 'u'
	V8SharedObject         V8SerializationTag = 'p'
	V8WasmModuleTransfer   V8SerializationTag = 'w'
	V8HostObject           V8SerializationTag = '\\'
	V8WasmMemoryTransfer   V8SerializationTag = 'm'
	V8Error                V8SerializationTag = 'r'
)

// GlobalMetadataKeyType defines keys for global metadata.
type GlobalMetadataKeyType byte

const (
	SchemaVersion          GlobalMetadataKeyType = 0
	MaxDatabaseID          GlobalMetadataKeyType = 1
	DataVersion            GlobalMetadataKeyType = 2
	RecoveryBlobJournal    GlobalMetadataKeyType = 3
	ActiveBlobJournal      GlobalMetadataKeyType = 4
	EarliestSweep          GlobalMetadataKeyType = 5
	EarliestCompactionTime GlobalMetadataKeyType = 6
	ScopesPrefix           GlobalMetadataKeyType = 50
	DatabaseFreeList       GlobalMetadataKeyType = 100
	DatabaseName           GlobalMetadataKeyType = 201
)

// DatabaseMetaDataKeyType defines keys for database-specific metadata.
type DatabaseMetaDataKeyType byte

const (
	OriginName                       DatabaseMetaDataKeyType = 0
	DBName                           DatabaseMetaDataKeyType = 1
	IDBStringVersionData             DatabaseMetaDataKeyType = 2
	MaxAllocatedObjectStoreID        DatabaseMetaDataKeyType = 3
	IDBIntegerVersion                DatabaseMetaDataKeyType = 4
	BlobNumberGeneratorCurrentNumber DatabaseMetaDataKeyType = 5
	ObjectStoreMetaData              DatabaseMetaDataKeyType = 50
	IndexMetaData                    DatabaseMetaDataKeyType = 100
	ObjectStoreFreeList              DatabaseMetaDataKeyType = 150
	IndexFreeList                    DatabaseMetaDataKeyType = 151
	ObjectStoreNames                 DatabaseMetaDataKeyType = 200
	IndexNames                       DatabaseMetaDataKeyType = 201
)

type DBMetadataKeyType = DatabaseMetaDataKeyType // Alias for brevity

// IDBKeyType defines the data type of an IndexedDB key component.
type IDBKeyType byte

const (
	IDBKeyNull   IDBKeyType = 0
	IDBKeyString IDBKeyType = 1
	IDBKeyDate   IDBKeyType = 2
	IDBKeyNumber IDBKeyType = 3
	IDBKeyArray  IDBKeyType = 4
	IDBKeyMinKey IDBKeyType = 5
	IDBKeyBinary IDBKeyType = 6
)

const (
	RequiresProcessingSSVPseudoVersion = 0x11
	ReplaceWithBlob                    = 0x01
	CompressedWithSnappy               = 0x02
)

// BlinkSerializationTag represents tags in Blink-serialized data.
type BlinkSerializationTag byte

const (
	BlinkVersionTag    BlinkSerializationTag = 0xFF
	BlinkBlob          BlinkSerializationTag = 'B'
	BlinkFile          BlinkSerializationTag = 'F'
	BlinkTrailerOffset BlinkSerializationTag = 0xFE

	// Add more Blink tags as needed
)
